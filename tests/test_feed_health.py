import time

from src.observability.feed_health import FeedHealthTracker


def test_feed_health_accepts_valid_bbo_without_levels():
    tracker = FeedHealthTracker()

    tracker.on_book_update(
        "BTC", "spot", best_bid=100.0, best_ask=101.0, ts=time.time(), bids=[], asks=[]
    )

    snapshot = tracker.build_asset_snapshot("BTC")

    assert snapshot["spot_incomplete"] is False
    assert snapshot["crossed"] is False
    assert snapshot["spot_bid"] == 100.0
    assert snapshot["spot_ask"] == 101.0


def test_feed_health_marks_crossed_bbo_incomplete():
    tracker = FeedHealthTracker()

    tracker.on_book_update(
        "BTC", "spot", best_bid=101.0, best_ask=100.0, ts=time.time(), bids=[], asks=[]
    )

    snapshot = tracker.build_asset_snapshot("BTC")

    assert snapshot["spot_incomplete"] is True
    assert snapshot["crossed"] is True
