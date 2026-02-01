import json
import time
import threading
import websocket

WS_URL = "wss://api.hyperliquid.xyz/ws"

def on_open(ws):
    sub = {
        "method": "subscribe",
        "subscription": {
            "type": "l2Book",
            "coin": "HYPE/USDC"   # SPOT (forma corretta da verificare)
        }
    }
    ws.send(json.dumps(sub))
    print("Subscribed to l2Book SPOT HYPE/USDC")

def on_message(ws, message):
    print("MSG:", message)

def on_error(ws, error):
    print("ERROR:", error)

def on_close(ws, *args):
    print("CLOSED", args)

ws = websocket.WebSocketApp(
    WS_URL,
    on_open=on_open,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close,
)

threading.Thread(target=ws.run_forever, daemon=True).start()
time.sleep(30)
