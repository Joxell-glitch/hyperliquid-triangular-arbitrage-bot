from src.hyperliquid_client.client import HyperliquidClient


def test_extract_spot_ws_coin_from_universe():
    universe = [
        {"name": "PURR/USDC", "index": 0},
        {"name": "FOO/USDC", "index": 3},
    ]

    ws_coin = HyperliquidClient.extract_spot_ws_coin_from_universe(universe, "PURR/USDC")

    assert ws_coin == "@0"
