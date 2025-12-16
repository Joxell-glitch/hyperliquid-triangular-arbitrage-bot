from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Tuple

from src.config.loader import load_config
from src.config.models import Settings, TradingSettings
from src.core.logging import get_logger
from src.db.models import SpotPerpOpportunity
from src.db.session import get_session
from src.hyperliquid_client.client import HyperliquidClient

logger = get_logger(__name__)


@dataclass
class BookSnapshot:
    best_bid: float = 0.0
    best_ask: float = 0.0

    @classmethod
    def from_levels(cls, bids: List[Tuple[float, float]], asks: List[Tuple[float, float]]):
        bid_price = max(bids, key=lambda x: x[0])[0] if bids else 0.0
        ask_price = min(asks, key=lambda x: x[0])[0] if asks else 0.0
        return cls(best_bid=bid_price, best_ask=ask_price)

    def has_liquidity(self) -> bool:
        return self.best_bid > 0 and self.best_ask > 0


@dataclass
class AssetState:
    spot: BookSnapshot = field(default_factory=BookSnapshot)
    perp: BookSnapshot = field(default_factory=BookSnapshot)
    mark_price: float = 0.0
    funding_rate: float = 0.0

    def ready(self) -> bool:
        return self.spot.has_liquidity() and self.perp.has_liquidity()


class SpotPerpPaperEngine:
    """
    Paper engine that observes spot and perp books to estimate arbitrage edge.

    No real orders are sent. The engine only logs and persists opportunities
    when the estimated net PnL is positive.
    """

    def __init__(
        self,
        client: HyperliquidClient,
        assets: Iterable[str],
        trading: TradingSettings,
        db_session_factory=get_session,
        taker_fee_spot: float = 0.001,
        taker_fee_perp: float = 0.0005,
    ) -> None:
        self.client = client
        self.assets = list(assets)
        self.trading = trading
        self.taker_fee_spot = taker_fee_spot
        self.taker_fee_perp = taker_fee_perp
        self.db_session_factory = db_session_factory
        self.asset_state: Dict[str, AssetState] = {asset: AssetState() for asset in self.assets}
        self._running = False

    async def run_forever(self, stop_event: Optional[asyncio.Event] = None) -> None:
        self._running = True
        await self.client.connect_ws()
        await self._subscribe_streams()

        async for msg in self.client.ws_messages():
            if stop_event and stop_event.is_set():
                break
            self._handle_message(msg)
        self._running = False

    async def _subscribe_streams(self) -> None:
        """Subscribe to spot and perp books plus mark/funding updates."""
        if not self.client._ws:
            raise RuntimeError("WebSocket not connected")
        subscriptions = []
        for asset in self.assets:
            subscriptions.append({"type": "l2Book", "coin": asset})
            subscriptions.append({"type": "l2Book", "coin": asset, "perp": True})
            subscriptions.append({"type": "markPrice", "coin": asset})
            subscriptions.append({"type": "funding", "coin": asset})
        await self.client._ws.send(json.dumps({"type": "subscribe", "subscriptions": subscriptions}))

    def _handle_message(self, msg: Dict) -> None:
        msg_type = msg.get("type")
        if msg_type == "l2Book":
            coin = msg.get("coin") or msg.get("asset")
            if not coin or coin not in self.asset_state:
                return
            levels = msg.get("levels", {})
            bids = levels.get("bids", [])
            asks = levels.get("asks", [])
            book = BookSnapshot.from_levels(bids, asks)
            if msg.get("perp") or msg.get("isPerp"):
                self.asset_state[coin].perp = book
            else:
                self.asset_state[coin].spot = book
            self._evaluate_and_record(coin)
        elif msg_type == "markPrice":
            coin = msg.get("coin")
            if coin in self.asset_state:
                self.asset_state[coin].mark_price = float(msg.get("mark", 0.0))
        elif msg_type == "funding":
            coin = msg.get("coin")
            if coin in self.asset_state:
                self.asset_state[coin].funding_rate = float(msg.get("fundingRate", 0.0))

    def _evaluate_and_record(self, asset: str) -> None:
        state = self.asset_state[asset]
        if not state.ready():
            return

        spot = state.spot
        perp = state.perp
        notional = max(self.trading.min_position_size, 1.0)
        funding_estimate = state.funding_rate * notional

        # Case A: spot undervalued
        if perp.best_bid > spot.best_ask > 0:
            spread_gross = (perp.best_bid - spot.best_ask) / spot.best_ask
            direction = "spot_long"
            spot_px = spot.best_ask
            perp_px = perp.best_bid
            spot_label = "spot_ask"
            perp_label = "perp_bid"
        # Case B: spot overvalued
        elif spot.best_bid > perp.best_ask > 0:
            spread_gross = (spot.best_bid - perp.best_ask) / spot.best_bid
            direction = "spot_short"
            spot_px = spot.best_bid
            perp_px = perp.best_ask
            spot_label = "spot_bid"
            perp_label = "perp_ask"
        else:
            return

        fee_spot = self.taker_fee_spot * notional
        fee_perp = self.taker_fee_perp * notional
        pnl_net = spread_gross * notional - fee_spot - fee_perp - funding_estimate

        if spread_gross <= 0 or pnl_net <= 0:
            return

        fee_total = fee_spot + fee_perp
        self._log_opportunity(
            asset=asset,
            direction=direction,
            spot_price=spot_px,
            perp_price=perp_px,
            mark_price=state.mark_price,
            spot_label=spot_label,
            perp_label=perp_label,
            spread_gross=spread_gross,
            fee_total=fee_total,
            funding_estimate=funding_estimate,
            pnl_net_estimated=pnl_net,
        )
        self._persist_opportunity(
            asset=asset,
            direction=direction,
            spot_price=spot_px,
            perp_price=perp_px,
            mark_price=state.mark_price,
            spread_gross=spread_gross,
            fee_estimated=fee_total,
            funding_estimated=funding_estimate,
            pnl_net_estimated=pnl_net,
        )

    def _log_opportunity(
        self,
        asset: str,
        direction: str,
        spot_price: float,
        perp_price: float,
        mark_price: float,
        spot_label: str,
        perp_label: str,
        spread_gross: float,
        fee_total: float,
        funding_estimate: float,
        pnl_net_estimated: float,
    ) -> None:
        logger.info(
            (
                "[SPOT_PERP]\n"
                "asset=%s\n"
                "%s=%.6f\n"
                "%s=%.6f\n"
                "mark=%.6f\n"
                "spread_gross=%+.2f%%\n"
                "fee_total=%.4f\n"
                "funding_est=%.4f\n"
                "pnl_net_est=%+.4f\n"
                "direction=%s"
            ),
            asset,
            spot_label,
            spot_price,
            perp_label,
            perp_price,
            mark_price,
            spread_gross * 100,
            fee_total,
            funding_estimate,
            pnl_net_estimated,
            direction,
        )

    def _persist_opportunity(
        self,
        asset: str,
        direction: str,
        spot_price: float,
        perp_price: float,
        mark_price: float,
        spread_gross: float,
        fee_estimated: float,
        funding_estimated: float,
        pnl_net_estimated: float,
    ) -> None:
        session = self.db_session_factory()
        with session as s:
            s.add(
                SpotPerpOpportunity(
                    timestamp=time.time(),
                    asset=asset,
                    direction=direction,
                    spot_price=spot_price,
                    perp_price=perp_price,
                    mark_price=mark_price,
                    spread_gross=spread_gross,
                    fee_estimated=fee_estimated,
                    funding_estimated=funding_estimated,
                    pnl_net_estimated=pnl_net_estimated,
                )
            )
            s.commit()


async def run_spot_perp_engine(
    assets: Iterable[str],
    settings: Optional[Settings] = None,
    taker_fee_spot: float = 0.001,
    taker_fee_perp: float = 0.0005,
):  
    settings = settings or load_config("config/config.yaml")
    client = HyperliquidClient(settings.api, settings.network)
    db_session_factory = get_session(settings)
    engine = SpotPerpPaperEngine(
        client,
        assets,
        settings.trading,
        db_session_factory=db_session_factory,
        taker_fee_spot=taker_fee_spot,
        taker_fee_perp=taker_fee_perp,
    )
    stop_event = asyncio.Event()

    try:
        await engine.run_forever(stop_event=stop_event)
    finally:
        stop_event.set()
        await client.close()
