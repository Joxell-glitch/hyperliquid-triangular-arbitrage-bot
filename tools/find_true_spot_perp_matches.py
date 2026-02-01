import requests, math

URL="https://api.hyperliquid.xyz/info"

# PERP mids
all_mids = requests.post(URL, json={"type":"allMids"}).json()  # { "BTC": "..." , ... }

# SPOT meta + ctxs
spot_meta, spot_ctxs = requests.post(URL, json={"type":"spotMetaAndAssetCtxs"}).json()
universe = spot_meta["universe"]
tokens = spot_meta["tokens"]

# helper: spot base name from universe entry
def spot_pair_name(ui):
    a,b = universe[ui]["tokens"]
    return tokens[a]["name"], tokens[b]["name"]

# scan all spot pairs quoted in USDC( tokenIndex 0 )
USDC_INDEX = 0

cands=[]
for ui,u in enumerate(universe):
    a,b = u["tokens"]
    if b != USDC_INDEX:
        continue
    base = tokens[a]["name"]
    # must exist as PERP name
    if base not in all_mids:
        continue

    # spot mid
    s_mid = float(spot_ctxs[ui].get("midPx") or 0.0)
    # perp mid
    p_mid = float(all_mids[base])

    if s_mid <= 0 or p_mid <= 0:
        continue

    ratio = p_mid / s_mid
    diff_bps = abs(ratio - 1.0) * 10000

    # keep reasonably close to 1
    if diff_bps <= 300:  # <= 3%
        cands.append((diff_bps, ui, base, s_mid, p_mid, ratio))

cands.sort(key=lambda x: x[0])

print("TOP matches (ratio close to 1):")
for diff_bps, ui, base, s_mid, p_mid, ratio in cands[:30]:
    print(f"{base:10s} spot=@{ui:<4d} spot_mid={s_mid:.8f} perp_mid={p_mid:.8f} ratio={ratio:.6f} diff={diff_bps:.2f} bps")

print("TOTAL:", len(cands))
