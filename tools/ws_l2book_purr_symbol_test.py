import json, time, threading, websocket

WS_URL="wss://api.hyperliquid.xyz/ws"
COIN="PURR/USDC"   # <-- simbolo spot (non @0)

def on_open(ws):
    print("OPEN")
    sub={"method":"subscribe","subscription":{"type":"l2Book","coin":COIN}}
    ws.send(json.dumps(sub))
    print("SUB", sub)

def on_message(ws,msg):
    print("MSG", msg)

def on_error(ws,e):
    print("ERROR", repr(e))

def on_close(ws,code,msg):
    print("CLOSE", code, msg)

ws=websocket.WebSocketApp(WS_URL,on_open=on_open,on_message=on_message,on_error=on_error,on_close=on_close)
threading.Thread(target=ws.run_forever, kwargs={"ping_interval":20,"ping_timeout":10}, daemon=True).start()
time.sleep(20)
