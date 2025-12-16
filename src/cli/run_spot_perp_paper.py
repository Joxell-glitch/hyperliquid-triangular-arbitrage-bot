from __future__ import annotations

import asyncio
from typing import Optional

from src.config.loader import load_config
from src.core.logging import get_logger, setup_logging
from src.db.session import get_session, init_db
from src.hyperliquid_client.client import HyperliquidClient
from src.strategy.spot_perp_paper import SpotPerpPaperEngine

logger = get_logger(__name__)


async def _run_engine(config_path: str) -> None:
    settings = load_config(config_path)
    setup_logging(settings.logging)
    init_db(settings)

    assets = settings.trading.whitelist or ["BTC", "ETH", "SOL"]
    logger.info("Starting spot-perp paper engine for assets: %s", ", ".join(assets))

    client = HyperliquidClient(settings.api, settings.network)
    session_factory = get_session(settings)
    engine = SpotPerpPaperEngine(client, assets, settings.trading, db_session_factory=session_factory)

    stop_event = asyncio.Event()
    try:
        await engine.run_forever(stop_event=stop_event)
    except KeyboardInterrupt:  # pragma: no cover - manual stop
        logger.info("Keyboard interrupt received, shutting down spot-perp engine")
        stop_event.set()
    finally:
        await client.close()
        logger.info("Spot-perp paper engine stopped")


def main(config_path: Optional[str] = "config/config.yaml") -> None:
    asyncio.run(_run_engine(config_path))


if __name__ == "__main__":
    main()
