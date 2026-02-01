"""
Universe Snapshot Export - D.2 (armed but not active)

Exports 24h snapshot of market_samples data to CSV and JSON reports.
Safe to run while collector is active (read-only, retry on lock).
"""

from __future__ import annotations

import csv
import json
import os
import sqlite3
import time
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import create_engine, func, text
from sqlalchemy.orm import sessionmaker

from src.core.logging import get_logger
from src.db.models import MarketSample

logger = get_logger(__name__)


def _retry_on_lock(func, max_retries: int = 3, delays: List[float] = [0.2, 0.5, 1.0]):
    """Retry function on database locked error."""
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            error_str = str(e).lower()
            if "locked" in error_str or "database is locked" in error_str:
                if attempt < max_retries - 1:
                    delay = delays[min(attempt, len(delays) - 1)]
                    logger.warning(
                        "[EXPORT] Database locked (attempt %d/%d), retrying in %.1fs",
                        attempt + 1,
                        max_retries,
                        delay,
                    )
                    time.sleep(delay)
                    continue
                else:
                    logger.error("[EXPORT] Database locked after %d attempts", max_retries)
                    raise
            else:
                raise
    return None


def export_universe_snapshot(
    db_path: str = "data/arb_bot.sqlite",
    table: str = "market_samples",
    min_window_hours: float = 24.0,
    out_dir: str = "exports/universe_snapshots",
    format: str = "csv",
    top_n: int = 400,
    force: bool = False,
    now_ms: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Export 24h snapshot of market_samples data.

    Returns dict with export results and metadata.
    """
    now_ms = now_ms or int(time.time() * 1000)
    timestamp_str = datetime.fromtimestamp(now_ms / 1000.0).strftime("%Y%m%d_%H%M%S")

    # Create output directory
    os.makedirs(out_dir, exist_ok=True)

    # Connect to database (read-only)
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    Session = sessionmaker(bind=engine, expire_on_commit=False)

    def _get_min_max_ts() -> Tuple[Optional[int], Optional[int]]:
        """Get min and max ts_ms from table."""
        session = Session()
        try:
            result = session.query(
                func.min(MarketSample.ts_ms).label("min_ts"),
                func.max(MarketSample.ts_ms).label("max_ts"),
            ).first()
            if result:
                return result.min_ts, result.max_ts
            return None, None
        finally:
            session.close()

    # Get min/max timestamps with retry
    min_ts_ms, max_ts_ms = _retry_on_lock(_get_min_max_ts)

    if min_ts_ms is None or max_ts_ms is None:
        logger.info("[EXPORT] No data in table %s, export skipped", table)
        return {
            "status": "skipped",
            "reason": "no_data",
            "db_path": db_path,
            "table": table,
        }

    window_ms = max_ts_ms - min_ts_ms
    window_hours = window_ms / (3600.0 * 1000.0)

    logger.info(
        "[EXPORT] Data window: %.2f hours (min_ts_ms=%d, max_ts_ms=%d)",
        window_hours,
        min_ts_ms,
        max_ts_ms,
    )

    # Check minimum window requirement
    if not force and window_hours < min_window_hours:
        logger.warning(
            "[EXPORT] NOT READY (window=%.2fh < %.2fh), export skipped",
            window_hours,
            min_window_hours,
        )
        return {
            "status": "skipped",
            "reason": "window_too_small",
            "window_hours": window_hours,
            "min_window_hours": min_window_hours,
            "db_path": db_path,
            "table": table,
        }

    # Calculate cutoff for last 24h
    cutoff_24h_ms = max_ts_ms - (24 * 3600 * 1000)

    def _export_raw_csv() -> int:
        """Export raw CSV with all columns."""
        session = Session()
        try:
            # Get all columns from MarketSample
            columns = [
                "id",
                "ts_ms",
                "base",
                "quote",
                "market_type",
                "variant",
                "symbol_raw",
                "bid",
                "ask",
                "mid",
                "spread_bps",
                "mark_price",
                "funding_rate",
                "open_interest_usd",
                "volume_24h_usd",
                "level",
                "score",
                "stale_flag",
            ]

            filename = os.path.join(out_dir, f"universe_24h_{timestamp_str}_raw.csv")
            rows_exported = 0

            with open(filename, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(columns)

                # Stream query results (chunked)
                query = (
                    session.query(MarketSample)
                    .filter(MarketSample.ts_ms >= cutoff_24h_ms)
                    .order_by(MarketSample.ts_ms)
                )

                # Process in chunks to avoid loading all in RAM
                chunk_size = 1000
                offset = 0
                while True:
                    chunk = query.offset(offset).limit(chunk_size).all()
                    if not chunk:
                        break

                    for sample in chunk:
                        row = [
                            sample.id,
                            sample.ts_ms,
                            sample.base,
                            sample.quote,
                            sample.market_type,
                            sample.variant,
                            sample.symbol_raw,
                            sample.bid,
                            sample.ask,
                            sample.mid,
                            sample.spread_bps,
                            sample.mark_price,
                            sample.funding_rate,
                            sample.open_interest_usd,
                            sample.volume_24h_usd,
                            sample.level,
                            sample.score,
                            sample.stale_flag,
                        ]
                        writer.writerow(row)
                        rows_exported += 1

                    offset += chunk_size
                    if len(chunk) < chunk_size:
                        break

            logger.info("[EXPORT] Raw CSV exported: %d rows to %s", rows_exported, filename)
            return rows_exported
        finally:
            session.close()

    def _export_top_levels_csv() -> int:
        """Export top N markets by score with latest record per symbol_raw."""
        session = Session()
        try:
            # Get latest record per symbol_raw (subquery)
            subquery = (
                session.query(
                    MarketSample.symbol_raw,
                    func.max(MarketSample.ts_ms).label("max_ts"),
                )
                .filter(MarketSample.ts_ms >= cutoff_24h_ms)
                .group_by(MarketSample.symbol_raw)
                .subquery()
            )

            # Join to get full records
            query = (
                session.query(MarketSample)
                .join(
                    subquery,
                    (MarketSample.symbol_raw == subquery.c.symbol_raw)
                    & (MarketSample.ts_ms == subquery.c.max_ts),
                )
                .order_by(MarketSample.score.desc().nullslast())
                .limit(top_n)
            )

            columns = [
                "symbol_raw",
                "market_type",
                "base",
                "quote",
                "variant",
                "level",
                "score",
                "spread_bps",
                "volume_24h_usd",
                "open_interest_usd",
            ]

            filename = os.path.join(out_dir, f"universe_24h_{timestamp_str}_top_levels.csv")
            rows_exported = 0

            with open(filename, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(columns)

                for sample in query.all():
                    row = [
                        sample.symbol_raw,
                        sample.market_type,
                        sample.base,
                        sample.quote,
                        sample.variant,
                        sample.level,
                        sample.score,
                        sample.spread_bps,
                        sample.volume_24h_usd,
                        sample.open_interest_usd,
                    ]
                    writer.writerow(row)
                    rows_exported += 1

            logger.info("[EXPORT] Top levels CSV exported: %d rows to %s", rows_exported, filename)
            return rows_exported
        finally:
            session.close()

    def _calculate_report_metrics() -> Dict[str, Any]:
        """Calculate report metrics."""
        session = Session()
        try:
            # Get all samples in last 24h
            samples = (
                session.query(MarketSample)
                .filter(MarketSample.ts_ms >= cutoff_24h_ms)
                .all()
            )

            rows_exported_raw = len(samples)

            # Distinct markets
            distinct_markets = set(s.symbol_raw for s in samples)

            # Null rates
            null_counts = defaultdict(int)
            total = len(samples)
            for sample in samples:
                if sample.bid is None:
                    null_counts["bid"] += 1
                if sample.ask is None:
                    null_counts["ask"] += 1
                if sample.mid is None:
                    null_counts["mid"] += 1
                if sample.spread_bps is None:
                    null_counts["spread_bps"] += 1
                if sample.volume_24h_usd is None:
                    null_counts["volume_24h_usd"] += 1
                if sample.open_interest_usd is None:
                    null_counts["open_interest_usd"] += 1
                if sample.funding_rate is None:
                    null_counts["funding_rate"] += 1

            null_rates = {
                field: (count / total if total > 0 else 0.0) for field, count in null_counts.items()
            }

            # Markets by level (from latest record per symbol_raw)
            subquery = (
                session.query(
                    MarketSample.symbol_raw,
                    func.max(MarketSample.ts_ms).label("max_ts"),
                )
                .filter(MarketSample.ts_ms >= cutoff_24h_ms)
                .group_by(MarketSample.symbol_raw)
                .subquery()
            )

            latest_samples = (
                session.query(MarketSample)
                .join(
                    subquery,
                    (MarketSample.symbol_raw == subquery.c.symbol_raw)
                    & (MarketSample.ts_ms == subquery.c.max_ts),
                )
                .all()
            )

            markets_by_level = defaultdict(int)
            for sample in latest_samples:
                level = sample.level or "D"
                markets_by_level[level] += 1

            return {
                "rows_exported_raw": rows_exported_raw,
                "rows_distinct_markets": len(distinct_markets),
                "null_rates": null_rates,
                "markets_by_level": dict(markets_by_level),
            }
        finally:
            session.close()

    def _export_report_json(metrics: Dict[str, Any]) -> None:
        """Export report JSON."""
        report = {
            "timestamp_export": now_ms / 1000.0,
            "db_path": db_path,
            "table": table,
            "min_ts_ms": min_ts_ms,
            "max_ts_ms": max_ts_ms,
            "window_hours": window_hours,
            "rows_exported_raw": metrics["rows_exported_raw"],
            "rows_distinct_markets": metrics["rows_distinct_markets"],
            "null_rates": metrics["null_rates"],
            "markets_by_level": metrics["markets_by_level"],
            "force": force,
        }

        filename = os.path.join(out_dir, f"universe_24h_{timestamp_str}_report.json")
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)

        logger.info("[EXPORT] Report JSON exported to %s", filename)

    # Execute exports with retry
    try:
        rows_exported_raw = _retry_on_lock(_export_raw_csv)
        rows_exported_top = _retry_on_lock(_export_top_levels_csv)
        metrics = _retry_on_lock(_calculate_report_metrics)
        _retry_on_lock(lambda: _export_report_json(metrics))

        return {
            "status": "success",
            "timestamp_str": timestamp_str,
            "rows_exported_raw": rows_exported_raw,
            "rows_exported_top": rows_exported_top,
            "window_hours": window_hours,
            "out_dir": out_dir,
        }
    except Exception as e:
        logger.error("[EXPORT] Error during export: %s", e, exc_info=True)
        return {
            "status": "error",
            "error": str(e),
        }

