"""
Universe Raw Collector - Workpack 2/2

Collects L1 orderbook data and context for all Hyperliquid markets (spot and perp).
Implements ranking, levels A/B/C/D, promotion/demotion with hysteresis, spread filtering,
and dynamic sampling scheduler.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import func
from sqlalchemy.orm import Session

from src.config.models import Settings
from src.core.logging import get_logger
from src.db.models import MarketSample
from src.db.session import get_session
from src.hyperliquid_client.client import HyperliquidClient

logger = get_logger(__name__)

# Level definitions
LEVEL_A_INTERVAL_MS = 250
LEVEL_B_INTERVAL_MS = 2000
LEVEL_C_INTERVAL_MS = 2000
LEVEL_D_INTERVAL_MS = 2000

# Spread filter thresholds
SPREAD_BPS_IGNORE_THRESHOLD = 30  # Ignore market completely
SPREAD_BPS_NO_A_THRESHOLD = 15  # Cannot be Level A

# Safety fallback assets
SAFETY_ASSETS = {"BTC", "ETH", "SOL"}


@dataclass
class MarketInfo:
    """Information about a market/variant."""

    base: str
    quote: str
    market_type: str  # "SPOT" | "PERP"
    variant: str  # "USDC", "USDE", etc.
    symbol_raw: str  # Primary logical ID


@dataclass
class MarketRanking:
    """Ranking information for a market."""

    symbol_raw: str
    rank: int
    score: float
    volume_24h_usd: Optional[float]
    open_interest_usd: Optional[float]
    spread_bps: Optional[float]


@dataclass
class MarketSchedule:
    """Scheduling information for a market."""

    symbol_raw: str
    level: str  # "A", "B", "C", "D"
    next_due_ms: int
    interval_ms: int


@dataclass
class SampleMetrics:
    """Runtime metrics for the collector."""

    rows_buffered: int = 0
    rows_inserted: int = 0
    inserts_count: int = 0
    insert_times: deque = None
    skipped_spread_gt_30: int = 0
    promotions_count: int = 0
    demotions_count: int = 0

    def __post_init__(self):
        if self.insert_times is None:
            self.insert_times = deque(maxlen=100)

    def record_insert(self, rows: int, duration: float):
        """Record an insert operation."""
        self.rows_inserted += rows
        self.inserts_count += 1
        self.insert_times.append(duration)
        if duration > 0:
            inserts_per_sec = rows / duration
            logger.debug(
                "[COLLECTOR] insert batch_size=%d duration=%.3fs inserts_per_sec=%.2f",
                rows,
                duration,
                inserts_per_sec,
            )

    def get_inserts_per_sec_avg(self) -> float:
        """Calculate average inserts per second."""
        if not self.insert_times:
            return 0.0
        total_rows = sum(
            self.insert_times[i] * (self.rows_inserted / max(self.inserts_count, 1))
            for i in range(len(self.insert_times))
        )
        total_time = sum(self.insert_times)
        if total_time == 0:
            return 0.0
        return total_rows / total_time if total_rows > 0 else 0.0


class UniverseRawCollector:
    """Collects raw market data for all Hyperliquid markets."""

    def __init__(self, settings: Settings, client: HyperliquidClient, max_concurrent_snapshots: int = 10):
        self.settings = settings
        self.client = client
        self.session_factory = get_session(settings)
        self.metrics = SampleMetrics()
        self.buffer: List[MarketSample] = []
        self.buffer_lock = asyncio.Lock()
        self.markets: List[MarketInfo] = []
        self.market_contexts: Dict[str, Dict[str, Any]] = {}
        self.last_cleanup_time = time.time()
        self.stop_event = asyncio.Event()
        # Semaphore to limit concurrent snapshot requests
        self.snapshot_semaphore = asyncio.Semaphore(max_concurrent_snapshots)

        # Ranking and level management (workpack2)
        self.rankings: Dict[str, MarketRanking] = {}  # symbol_raw -> ranking
        self.schedules: Dict[str, MarketSchedule] = {}  # symbol_raw -> schedule
        self.last_ranking_time = 0.0
        self.ranking_refresh_sec = 60.0  # Default, can be overridden

        # Promotion/demotion hysteresis tracking
        self.promotion_counters: Dict[str, int] = defaultdict(int)  # symbol_raw -> consecutive count
        self.demotion_counters: Dict[str, int] = defaultdict(int)  # symbol_raw -> consecutive count
        self.last_promotions: List[str] = []  # Track promotions in last window
        self.last_demotions: List[str] = []  # Track demotions in last window
        self.last_window_start = time.time()

    async def discover_markets(self) -> List[MarketInfo]:
        """Discover all markets from spot and perp metadata."""
        markets: List[MarketInfo] = []

        # Fetch spot metadata
        try:
            spot_meta_raw = await self.client.fetch_spot_meta_and_asset_ctxs()
            # Handle list response (Hyperliquid sometimes returns list)
            if isinstance(spot_meta_raw, list):
                spot_meta = {}
                for item in spot_meta_raw:
                    if isinstance(item, dict):
                        for key in ("universe", "tokens", "spotMeta", "assetCtxs"):
                            if key in item and key not in spot_meta:
                                spot_meta[key] = item[key]
            else:
                spot_meta = spot_meta_raw if isinstance(spot_meta_raw, dict) else {}

            spot_universe = spot_meta.get("universe") or []
            spot_meta_data = spot_meta.get("spotMeta", {})
            if isinstance(spot_meta_data, dict):
                spot_universe = spot_meta_data.get("universe") or spot_universe

            tokens = spot_meta.get("tokens") or []
            if isinstance(spot_meta_data, dict):
                tokens = tokens + (spot_meta_data.get("tokens") or [])

            token_map = {
                token.get("index"): str(token.get("name")).upper()
                for token in tokens
                if token.get("index") is not None and token.get("name")
            }

            # Process spot markets
            for entry in spot_universe:
                if not isinstance(entry, dict):
                    continue

                entry_tokens = entry.get("tokens")
                if entry_tokens and len(entry_tokens) == 2 and token_map:
                    base_id, quote_id = entry_tokens
                    base = token_map.get(base_id)
                    quote = token_map.get(quote_id)
                else:
                    base = entry.get("base") or entry.get("coin") or entry.get("name")
                    quote = entry.get("quote") or "USDC"

                if not base or not quote:
                    continue

                base = str(base).upper()
                quote = str(quote).upper()

                # Extract variant from quote (USDC, USDE, etc.)
                variant = quote
                pair_name = entry.get("name") or entry.get("pair") or f"{base}/{quote}"
                symbol_raw = pair_name

                markets.append(
                    MarketInfo(
                        base=base,
                        quote=quote,
                        market_type="SPOT",
                        variant=variant,
                        symbol_raw=symbol_raw,
                    )
                )

            # Store spot asset contexts
            for ctx in spot_meta.get("assetCtxs", []) or []:
                if not isinstance(ctx, dict):
                    continue
                coin = ctx.get("coin") or ctx.get("base") or ctx.get("name")
                if coin:
                    self.market_contexts[str(coin).upper()] = ctx

        except Exception as e:
            logger.error("[COLLECTOR] Error fetching spot meta: %s", e, exc_info=True)

        # Fetch perp metadata with contexts
        try:
            perp_meta_raw = await self.client.fetch_perp_meta_and_asset_ctxs()
            # Handle list response (Hyperliquid sometimes returns list)
            if isinstance(perp_meta_raw, list):
                perp_meta = {}
                for item in perp_meta_raw:
                    if isinstance(item, dict):
                        for key in ("universe", "tokens", "assetCtxs"):
                            if key in item and key not in perp_meta:
                                perp_meta[key] = item[key]
            else:
                perp_meta = perp_meta_raw if isinstance(perp_meta_raw, dict) else {}

            perp_universe = perp_meta.get("universe") or []

            # Process perp markets
            for entry in perp_universe:
                if not isinstance(entry, dict):
                    continue

                symbol = entry.get("name") or entry.get("symbol") or entry.get("coin") or entry.get("base")
                if not symbol:
                    continue

                base = str(symbol).upper()
                # For perp, variant is typically determined by the quote (USDC/USDE)
                # We'll use the base symbol and infer variant from context if available
                variant = "USDC"  # Default, can be overridden by context
                symbol_raw = base  # Perp symbol is typically just the base

                markets.append(
                    MarketInfo(
                        base=base,
                        quote="USD",  # Perp is always USD-denominated
                        market_type="PERP",
                        variant=variant,
                        symbol_raw=symbol_raw,
                    )
                )

            # Store perp asset contexts (OI, funding, etc.)
            for ctx in perp_meta.get("assetCtxs", []) or []:
                if not isinstance(ctx, dict):
                    continue
                coin = ctx.get("coin") or ctx.get("base") or ctx.get("name")
                if coin:
                    self.market_contexts[str(coin).upper()] = ctx

        except Exception as e:
            logger.error("[COLLECTOR] Error fetching perp meta: %s", e, exc_info=True)

        logger.info("[COLLECTOR] Discovered %d markets (spot + perp)", len(markets))
        return markets

    def _calculate_percentile_rank(self, value: float, all_values: List[float]) -> float:
        """Calculate percentile rank (0.0 to 1.0) for a value."""
        if not all_values:
            return 0.0
        sorted_values = sorted(all_values)
        count_below = sum(1 for v in sorted_values if v < value)
        count_equal = sum(1 for v in sorted_values if v == value)
        return (count_below + count_equal / 2.0) / len(sorted_values)

    def _calculate_rankings(self) -> Dict[str, MarketRanking]:
        """Calculate rankings for all markets based on latest DB data."""
        try:
            with self.session_factory() as session:
                # Get latest samples for each market (last 5 minutes for freshness)
                cutoff_ms = int((time.time() - 300) * 1000)
                latest_samples = (
                    session.query(MarketSample)
                    .filter(MarketSample.ts_ms >= cutoff_ms)
                    .order_by(MarketSample.ts_ms.desc())
                    .all()
                )

                # Group by symbol_raw and get most recent
                market_data: Dict[str, MarketSample] = {}
                for sample in latest_samples:
                    if sample.symbol_raw not in market_data:
                        market_data[sample.symbol_raw] = sample

                # Collect metrics for percentile calculation
                volumes: List[float] = []
                ois: List[float] = []
                spreads: List[float] = []

                for sample in market_data.values():
                    if sample.volume_24h_usd is not None:
                        volumes.append(sample.volume_24h_usd)
                    if sample.open_interest_usd is not None:
                        ois.append(sample.open_interest_usd)
                    if sample.spread_bps is not None:
                        spreads.append(sample.spread_bps)

                # Calculate rankings
                rankings: Dict[str, MarketRanking] = {}
                market_scores: List[Tuple[str, float]] = []

                for symbol_raw, sample in market_data.items():
                    volume = sample.volume_24h_usd or 0.0
                    oi = sample.open_interest_usd or 0.0
                    spread = sample.spread_bps or 0.0

                    # Calculate percentile ranks
                    volume_norm = self._calculate_percentile_rank(volume, volumes) if volumes else 0.0
                    oi_norm = self._calculate_percentile_rank(oi, ois) if ois else 0.0
                    spread_norm = self._calculate_percentile_rank(spread, spreads) if spreads else 0.0

                    # Score formula: 0.6 * volume + 0.3 * oi - 0.1 * spread (spread penalizes)
                    score = 0.6 * volume_norm + 0.3 * oi_norm - 0.1 * spread_norm

                    rankings[symbol_raw] = MarketRanking(
                        symbol_raw=symbol_raw,
                        rank=0,  # Will be set after sorting
                        score=score,
                        volume_24h_usd=sample.volume_24h_usd,
                        open_interest_usd=sample.open_interest_usd,
                        spread_bps=sample.spread_bps,
                    )
                    market_scores.append((symbol_raw, score))

                # Sort by score descending and assign ranks
                market_scores.sort(key=lambda x: x[1], reverse=True)
                for rank, (symbol_raw, _) in enumerate(market_scores, start=1):
                    if symbol_raw in rankings:
                        rankings[symbol_raw].rank = rank

                logger.info("[COLLECTOR] Calculated rankings for %d markets", len(rankings))
                return rankings

        except Exception as e:
            logger.error("[COLLECTOR] Error calculating rankings: %s", e, exc_info=True)
            return {}

    def _assign_level_from_rank(self, symbol_raw: str, rank: int, spread_bps: Optional[float]) -> str:
        """Assign level based on rank and spread constraints."""
        # Spread filter: cannot be Level A if spread > 15
        if spread_bps is not None and spread_bps > SPREAD_BPS_NO_A_THRESHOLD:
            if rank <= 100:
                # Would be A, but spread too high -> B
                return "B"
        elif spread_bps is None:
            # Missing spread -> cannot be A
            if rank <= 100:
                return "B"

        # Normal level assignment
        if rank <= 100:
            return "A"
        elif rank <= 200:
            return "B"
        elif rank <= 300:
            return "C"
        elif rank <= 400:
            return "D"
        else:
            return "D"

    def _update_levels_with_hysteresis(self, rankings: Dict[str, MarketRanking]) -> None:
        """Update levels with promotion/demotion hysteresis."""
        now = time.time()
        window_duration = 60.0  # 1 minute window for tracking

        # Reset window if needed
        if now - self.last_window_start >= window_duration:
            self.last_promotions = []
            self.last_demotions = []
            self.last_window_start = now

        promotions_this_cycle: List[str] = []
        demotions_this_cycle: List[str] = []

        for symbol_raw, ranking in rankings.items():
            current_level = self.schedules.get(symbol_raw, MarketSchedule(symbol_raw, "D", 0, LEVEL_D_INTERVAL_MS)).level
            rank = ranking.rank
            spread_bps = ranking.spread_bps

            # Promotion to Level A logic
            if current_level != "A":
                if rank <= 100 and (spread_bps is None or spread_bps <= SPREAD_BPS_NO_A_THRESHOLD):
                    self.promotion_counters[symbol_raw] += 1
                    if self.promotion_counters[symbol_raw] >= 3:
                        # Promote to A
                        new_level = "A"
                        if new_level != current_level:
                            promotions_this_cycle.append(symbol_raw)
                            logger.info(
                                "[COLLECTOR] Promotion: %s -> %s (rank=%d, spread=%.2f)",
                                symbol_raw,
                                new_level,
                                rank,
                                spread_bps or 0.0,
                            )
                        self.schedules[symbol_raw] = MarketSchedule(
                            symbol_raw, new_level, 0, LEVEL_A_INTERVAL_MS
                        )
                        self.promotion_counters[symbol_raw] = 0  # Reset after promotion
                else:
                    # Condition not met, reset counter
                    self.promotion_counters[symbol_raw] = 0

            # Demotion from Level A logic
            elif current_level == "A":
                should_demote = False
                if rank > 120:
                    self.demotion_counters[symbol_raw] += 1
                    if self.demotion_counters[symbol_raw] >= 3:
                        should_demote = True
                elif spread_bps is not None and spread_bps > 20:
                    self.demotion_counters[symbol_raw] += 1
                    if self.demotion_counters[symbol_raw] >= 3:
                        should_demote = True
                else:
                    # Condition not met, reset counter
                    self.demotion_counters[symbol_raw] = 0

                if should_demote:
                    # Demote from A
                    new_level = self._assign_level_from_rank(symbol_raw, rank, spread_bps)
                    if new_level != current_level:
                        demotions_this_cycle.append(symbol_raw)
                        logger.info(
                            "[COLLECTOR] Demotion: %s -> %s (rank=%d, spread=%.2f)",
                            symbol_raw,
                            new_level,
                            rank,
                            spread_bps or 0.0,
                        )
                    self.schedules[symbol_raw] = MarketSchedule(
                        symbol_raw, new_level, 0, self._get_interval_for_level(new_level)
                    )
                    self.demotion_counters[symbol_raw] = 0  # Reset after demotion

            # For non-A levels, assign directly from rank (no hysteresis)
            else:
                new_level = self._assign_level_from_rank(symbol_raw, rank, spread_bps)
                if new_level != current_level:
                    logger.debug(
                        "[COLLECTOR] Level change: %s -> %s (rank=%d)",
                        symbol_raw,
                        new_level,
                        rank,
                    )
                self.schedules[symbol_raw] = MarketSchedule(
                    symbol_raw, new_level, 0, self._get_interval_for_level(new_level)
                )

        # Update window tracking
        self.last_promotions.extend(promotions_this_cycle)
        self.last_demotions.extend(demotions_this_cycle)
        self.metrics.promotions_count += len(promotions_this_cycle)
        self.metrics.demotions_count += len(demotions_this_cycle)

    def _get_interval_for_level(self, level: str) -> int:
        """Get sampling interval in milliseconds for a level."""
        if level == "A":
            return LEVEL_A_INTERVAL_MS
        elif level == "B":
            return LEVEL_B_INTERVAL_MS
        elif level == "C":
            return LEVEL_C_INTERVAL_MS
        else:  # D or unknown
            return LEVEL_D_INTERVAL_MS

    def _apply_fallback_safety(self, rankings: Dict[str, MarketRanking]) -> None:
        """Apply fallback safety: ensure BTC/ETH/SOL are at least Level B."""
        fallback_active = False
        fallback_reason = ""

        # Check if we have enough rankings
        if len(rankings) < 10:
            fallback_active = True
            fallback_reason = "insufficient_rankings"

        # Ensure safety assets are included
        for market in self.markets:
            if market.base in SAFETY_ASSETS:
                symbol_raw = market.symbol_raw
                if symbol_raw not in rankings:
                    fallback_active = True
                    if fallback_reason:
                        fallback_reason += ",missing_safety_assets"
                    else:
                        fallback_reason = "missing_safety_assets"

                # Ensure minimum Level B
                current_schedule = self.schedules.get(symbol_raw)
                if current_schedule:
                    if current_schedule.level not in ("A", "B"):
                        # Force to B (never force to A)
                        self.schedules[symbol_raw] = MarketSchedule(
                            symbol_raw, "B", 0, LEVEL_B_INTERVAL_MS
                        )
                        logger.info(
                            "[COLLECTOR][FALLBACK] %s forced to Level B (safety asset)",
                            symbol_raw,
                        )

        if fallback_active:
            logger.warning(
                "[COLLECTOR][FALLBACK] Fallback safety active: %s",
                fallback_reason,
            )

    async def _refresh_rankings(self) -> None:
        """Refresh rankings and update levels."""
        rankings = self._calculate_rankings()
        if not rankings:
            logger.warning("[COLLECTOR] No rankings calculated, using fallback")
            self._apply_fallback_safety({})
            return

        self.rankings = rankings
        self._update_levels_with_hysteresis(rankings)
        self._apply_fallback_safety(rankings)

        # Initialize schedules for markets not yet scheduled
        for market in self.markets:
            if market.symbol_raw not in self.schedules:
                ranking = rankings.get(market.symbol_raw)
                if ranking:
                    level = self._assign_level_from_rank(market.symbol_raw, ranking.rank, ranking.spread_bps)
                else:
                    level = "D"  # Default for unranked
                self.schedules[market.symbol_raw] = MarketSchedule(
                    market.symbol_raw, level, 0, self._get_interval_for_level(level)
                )

    async def collect_sample(self, market: MarketInfo) -> Optional[MarketSample]:
        """Collect a single sample for a market."""
        ts_ms = int(time.time() * 1000)

        # Fetch L1 orderbook snapshot
        bid: Optional[float] = None
        ask: Optional[float] = None
        mid: Optional[float] = None
        spread_bps: Optional[float] = None
        stale_flag = False

        try:
            # Determine coin/symbol for orderbook fetch
            if market.market_type == "SPOT":
                coin = market.symbol_raw  # e.g., "BTC/USDC"
            else:
                coin = market.base  # e.g., "BTC"

            # Limit concurrent snapshot requests to avoid 429 errors
            async with self.snapshot_semaphore:
                snapshot = await self.client.fetch_orderbook_snapshot(
                    coin, asset=coin, kind=market.market_type.lower()
                )

            if snapshot:
                payload = self._extract_payload(snapshot)
                levels = payload.get("levels") or payload

                bids_source: Any = None
                asks_source: Any = None

                if isinstance(levels, dict):
                    bids_source = levels.get("bids")
                    asks_source = levels.get("asks")
                elif isinstance(levels, (list, tuple)) and len(levels) >= 2:
                    bids_source, asks_source = levels[0], levels[1]

                bids = (
                    bids_source
                    if isinstance(bids_source, list)
                    else payload.get("bids")
                    if isinstance(payload.get("bids"), list)
                    else []
                )
                asks = (
                    asks_source
                    if isinstance(asks_source, list)
                    else payload.get("asks")
                    if isinstance(payload.get("asks"), list)
                    else []
                )

                if bids and asks:
                    best_bid = self._best_price(bids, reverse=True)
                    best_ask = self._best_price(asks, reverse=False)
                    if best_bid and best_ask and best_bid > 0 and best_ask > 0:
                        bid = float(best_bid)
                        ask = float(best_ask)
                        mid = (bid + ask) / 2.0
                        if mid > 0:
                            spread_bps = ((ask - bid) / mid) * 10000.0
        except Exception as e:
            logger.debug("[COLLECTOR] Error fetching orderbook for %s: %s", market.symbol_raw, e)
            stale_flag = True

        # Spread filter: ignore if spread > 30
        if spread_bps is not None and spread_bps > SPREAD_BPS_IGNORE_THRESHOLD:
            self.metrics.skipped_spread_gt_30 += 1
            logger.debug(
                "[COLLECTOR] Skipping %s: spread_bps=%.2f > %d",
                market.symbol_raw,
                spread_bps,
                SPREAD_BPS_IGNORE_THRESHOLD,
            )
            return None  # Don't insert this sample

        # Extract context data
        mark_price: Optional[float] = None
        funding_rate: Optional[float] = None
        open_interest_usd: Optional[float] = None
        volume_24h_usd: Optional[float] = None

        ctx = self.market_contexts.get(market.base)
        if ctx:
            try:
                if market.market_type == "PERP":
                    mark_price = self._parse_float(ctx.get("markPx") or ctx.get("markPrice"))
                    funding_rate = self._parse_float(ctx.get("funding") or ctx.get("fundingRate"))
                    open_interest_usd = self._parse_float(
                        ctx.get("openInterest") or ctx.get("openInterestUsd")
                    )
                    volume_24h_usd = self._parse_float(
                        ctx.get("dayNtlVlm") or ctx.get("dayNotionalVolume") or ctx.get("volume24hUsd")
                    )
                else:  # SPOT
                    volume_24h_usd = self._parse_float(
                        ctx.get("dayNtlVlm") or ctx.get("dayNotionalVolume") or ctx.get("volume24hUsd")
                    )
            except Exception as e:
                logger.debug("[COLLECTOR] Error parsing context for %s: %s", market.symbol_raw, e)

        # If bid/ask are missing, mark as stale
        if bid is None or ask is None:
            stale_flag = True

        # Get current level and score from rankings
        ranking = self.rankings.get(market.symbol_raw)
        level = "D"  # Default
        score = None
        if ranking:
            score = ranking.score
            schedule = self.schedules.get(market.symbol_raw)
            if schedule:
                level = schedule.level

        sample = MarketSample(
            ts_ms=ts_ms,
            base=market.base,
            quote=market.quote,
            market_type=market.market_type,
            variant=market.variant,
            symbol_raw=market.symbol_raw,
            bid=bid,
            ask=ask,
            mid=mid,
            spread_bps=spread_bps,
            mark_price=mark_price,
            funding_rate=funding_rate,
            open_interest_usd=open_interest_usd,
            volume_24h_usd=volume_24h_usd,
            level=level,
            score=score,
            stale_flag=stale_flag,
        )

        return sample

    def _extract_payload(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        """Extract payload from Hyperliquid message."""
        if isinstance(msg, dict):
            return msg.get("data") or msg
        return {}

    def _best_price(self, levels: List[Any], reverse: bool = False) -> Optional[float]:
        """Extract best price from orderbook levels."""
        if not levels:
            return None
        try:
            if isinstance(levels[0], (list, tuple)) and len(levels[0]) >= 1:
                prices = [float(level[0]) for level in levels if level]
            elif isinstance(levels[0], dict):
                prices = [
                    float(level.get("px") or level.get("price") or 0)
                    for level in levels
                    if level.get("px") or level.get("price")
                ]
            else:
                return None
            if not prices:
                return None
            return max(prices) if reverse else min(prices)
        except Exception:
            return None

    def _parse_float(self, value: Any) -> Optional[float]:
        """Safely parse float value."""
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    async def flush_buffer(self) -> int:
        """Flush buffer to database in a single transaction."""
        async with self.buffer_lock:
            if not self.buffer:
                return 0

            samples = self.buffer[:]
            self.buffer.clear()
            self.metrics.rows_buffered = 0

        start_time = time.time()
        rows_inserted = 0

        try:
            with self.session_factory() as session:
                session.add_all(samples)
                session.commit()
                rows_inserted = len(samples)
        except Exception as e:
            logger.error("[COLLECTOR] Error flushing buffer: %s", e, exc_info=True)
            # Re-add samples to buffer on error (optional, could drop)
            async with self.buffer_lock:
                self.buffer.extend(samples)
                self.metrics.rows_buffered = len(self.buffer)

        duration = time.time() - start_time
        self.metrics.record_insert(rows_inserted, duration)

        return rows_inserted

    async def add_to_buffer(self, sample: MarketSample):
        """Add sample to buffer."""
        async with self.buffer_lock:
            self.buffer.append(sample)
            self.metrics.rows_buffered = len(self.buffer)

    async def cleanup_old_data(self, cleanup_sec: float = 300.0):
        """Cleanup data older than 24 hours (FIFO rolling window)."""
        now = time.time()
        if now - self.last_cleanup_time < cleanup_sec:
            return

        self.last_cleanup_time = now
        cutoff_ms = int((now - 24 * 3600) * 1000)

        try:
            with self.session_factory() as session:
                deleted = session.query(MarketSample).filter(MarketSample.ts_ms < cutoff_ms).delete()
                session.commit()
                if deleted > 0:
                    logger.info("[COLLECTOR] Cleanup: deleted %d rows older than 24h", deleted)
        except Exception as e:
            logger.error("[COLLECTOR] Error during cleanup: %s", e, exc_info=True)

    def get_db_rows_24h(self) -> int:
        """Get count of rows in last 24 hours."""
        try:
            cutoff_ms = int((time.time() - 24 * 3600) * 1000)
            with self.session_factory() as session:
                count = session.query(MarketSample).filter(MarketSample.ts_ms >= cutoff_ms).count()
                return count
        except Exception as e:
            logger.error("[COLLECTOR] Error counting DB rows: %s", e, exc_info=True)
            return 0

    def write_status_json(self):
        """Write status JSON files."""
        os.makedirs("data", exist_ok=True)

        # Count markets by level
        markets_by_level = {"A": 0, "B": 0, "C": 0, "D": 0}
        for schedule in self.schedules.values():
            level = schedule.level
            if level in markets_by_level:
                markets_by_level[level] += 1

        # universe_status.json
        status = {
            "timestamp": time.time(),
            "markets_total": len(self.markets),
            "markets_by_level": markets_by_level,
            "promotions_last_window": len(self.last_promotions),
            "demotions_last_window": len(self.last_demotions),
            "skipped_spread_gt_30_last_window": self.metrics.skipped_spread_gt_30,
            "inserts_per_sec_avg": self.metrics.get_inserts_per_sec_avg(),
            "db_rows_24h": self.get_db_rows_24h(),
        }

        with open("data/universe_status.json", "w") as f:
            json.dump(status, f, indent=2)

        # universe_levels.json (top 400)
        top_rankings = sorted(self.rankings.values(), key=lambda r: r.rank)[:400]
        levels_data = {
            "timestamp": time.time(),
            "markets_total": len(self.markets),
            "top_400": [
                {
                    "symbol_raw": r.symbol_raw,
                    "rank": r.rank,
                    "score": r.score,
                    "level": self.schedules.get(r.symbol_raw, MarketSchedule(r.symbol_raw, "D", 0, 0)).level,
                }
                for r in top_rankings
            ],
            "truncated": len(self.rankings) > 400,
        }

        with open("data/universe_levels.json", "w") as f:
            json.dump(levels_data, f, indent=2)

    def _get_markets_due_for_sampling(self, now_ms: int) -> List[MarketInfo]:
        """Get markets that are due for sampling based on their level schedule."""
        due_markets: List[MarketInfo] = []
        for market in self.markets:
            schedule = self.schedules.get(market.symbol_raw)
            if not schedule:
                # Not scheduled yet, add to due list (will be scheduled)
                due_markets.append(market)
                continue

            if now_ms >= schedule.next_due_ms:
                due_markets.append(market)
                # Update next_due_ms
                schedule.next_due_ms = now_ms + schedule.interval_ms

        return due_markets

    async def run(
        self,
        cleanup_sec: float = 300.0,
        duration_sec: Optional[float] = None,
        ranking_refresh_sec: Optional[float] = None,
    ):
        """Main collection loop."""
        logger.info("[COLLECTOR] Starting universe raw collector")
        self.ranking_refresh_sec = ranking_refresh_sec or 60.0
        logger.info("[COLLECTOR] ranking_refresh_sec=%s", self.ranking_refresh_sec)

        # Discover markets
        self.markets = await self.discover_markets()
        if not self.markets:
            logger.warning("[COLLECTOR] No markets discovered, exiting")
            return

        # Initial ranking refresh
        await self._refresh_rankings()

        start_time = time.time()
        min_sleep_ms = 50  # Minimum sleep to avoid busy-wait

        try:
            while not self.stop_event.is_set():
                if duration_sec and (time.time() - start_time) >= duration_sec:
                    logger.info("[COLLECTOR] Duration limit reached, stopping")
                    break

                now_ms = int(time.time() * 1000)

                # Refresh rankings periodically
                if time.time() - self.last_ranking_time >= self.ranking_refresh_sec:
                    await self._refresh_rankings()
                    self.last_ranking_time = time.time()

                # Get markets due for sampling
                due_markets = self._get_markets_due_for_sampling(now_ms)

                # Collect samples for due markets
                if due_markets:
                    tasks = [self.collect_sample(market) for market in due_markets]
                    samples = await asyncio.gather(*tasks, return_exceptions=True)

                    for sample in samples:
                        if isinstance(sample, Exception):
                            logger.debug("[COLLECTOR] Sample collection error: %s", sample)
                            continue
                        if sample:
                            await self.add_to_buffer(sample)

                # Flush buffer periodically
                async with self.buffer_lock:
                    buffer_size = len(self.buffer)
                if buffer_size >= 50:  # Flush when buffer reaches 50 samples
                    await self.flush_buffer()

                # Cleanup old data
                await self.cleanup_old_data(cleanup_sec)

                # Write status JSON
                self.write_status_json()

                # Sleep to avoid busy-wait
                await asyncio.sleep(min_sleep_ms / 1000.0)

        except Exception as e:
            logger.error("[COLLECTOR] Error in main loop: %s", e, exc_info=True)
        finally:
            # Final flush
            await self.flush_buffer()
            self.write_status_json()
            logger.info("[COLLECTOR] Collector stopped")
