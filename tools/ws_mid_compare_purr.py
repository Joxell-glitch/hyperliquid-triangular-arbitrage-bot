import json, time, threading, websocket

WS_URL="wss://api.hyperliquid.xyz/ws"
SPOT="PURR/USDC"   # spot coin (IMPORTANTE: non @0)
PERP="PURR"        # perp coin

st={"s":None,"p":None}

def bb_ba(levels):
    b=levels[0] if levels else []
    a=levels[1] if levels and len(levels)>1 else []
    bb=float(b[0]["px"]) if b else None
    ba=float(a[0]["px"]) if a else None
    return bb,ba

def emit():
    s=st["s"]; p=st["p"]
    if not s or not p: return
    if None in (s["bb"],s["ba"],p["bb"],p["ba"]): return
    ms=(s["bb"]+s["ba"])/2
    mp=(p["bb"]+p["ba"])/2
    spr=mp-ms
    bps=spr/ms*10000
    now=int(time.time()*1000)
    print(f"SPOT {SPOT} t={s['t']} age={now-s['t']}ms mid={ms:.8f} | "
          f"PERP {PERP} t={p['t']} age={now-p['t']}ms mid={mp:.8f} | "
          f"perp-spot={spr:.8f} ({bps:.2f} bps)")

def on_open(ws):
    ws.send(json.dumps({"method":"subscribe","subscription":{"type":"l2Book","coin":SPOT}}))
    ws.send(json.dumps({"method":"subscribe","subscription":{"type":"l2Book","coin":PERP}}))
    print(f"SUBMITTED spot={SPOT} perp={PERP}")

def on_message(ws,msg):
    j=json.loads(msg)
    if j.get("channel")!="l2Book": return
    d=j["data"]; c=d["coin"]; t=d["time"]; lv=d["levels"]
    bb,ba=bb_ba(lv)
    if c==SPOT: st["s"]={"t":t,"bb":bb,"ba":ba}
    elif c==PERP: st["p"]={"t":t,"bb":bb,"ba":ba}
    else: return
    emit()

def on_error(ws,e): print("ERROR",repr(e))
def on_close(ws,code,msg): print("CLOSE",code,msg)

ws=websocket.WebSocketApp(WS_URL,on_open=on_open,on_message=on_message,on_error=on_error,on_close=on_close)
threading.Thread(target=ws.run_forever, kwargs={"ping_interval":20,"ping_timeout":10}, daemon=True).start()
time.sleep(20)
