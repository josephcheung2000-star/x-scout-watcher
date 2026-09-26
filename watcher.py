#!/usr/bin/env python3
"""Fast listing watcher (runs on GitHub Actions every ~5 min). Pings Telegram the moment a high-impact
exchange listing appears: Binance spot listing / HODLer Airdrops, OKX spot listing, Upbit new market, Bybit spot
listing. The 3-hourly X-scout run does the deep analysis; this only buys speed.
State: seen.json (committed back by the workflow only when it changes). Each exchange is seeded silently the first
time it responds. An item is marked seen only after its ping was sent. A source failing 3 runs in a row triggers one
notice; one more notice when it recovers.
Env: TG_TOKEN, TG_CHAT."""
import json, os, re, time, urllib.request, urllib.parse
from datetime import datetime, timezone, timedelta

UA = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
SEEN_F = "seen.json"
JST = timezone(timedelta(hours=9))
FAIL_N = 3
FALLBACK = {"Binance": "https://www.binance.com/en/support/announcement/list/48", "OKX": "https://www.okx.com/help/section/announcements-new-listings",
            "Upbit": "https://upbit.com/service_center/notice", "Bybit": "https://announcements.bybit.com/en/?category=new_crypto"}

def get(url):
    with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=20) as r:
        return json.loads(r.read())

def tick(t):
    m = re.search(r"\(([A-Z0-9]{2,12})\)", t)
    return m.group(1) if m else None

def binance():
    d = get("https://www.binance.com/bapi/composite/v1/public/cms/article/list/query?type=1&catalogId=48&pageNo=1&pageSize=20")
    out = []
    for c in d["data"]["catalogs"]:
        if c["catalogId"] != 48: continue
        for a in c["articles"]:
            t = a["title"]
            if re.search(r"(?i)(binance will list|hodler airdrops)", t):
                out.append({"key": "binance:" + a["code"], "ex": "Binance", "title": t, "ts": a["releaseDate"] / 1000,
                            "url": f"https://www.binance.com/en/support/announcement/{a['code']}", "sym": tick(t)})
    return out

def okx():
    d = get("https://www.okx.com/api/v5/support/announcements?annType=announcements-new-listings")
    out = []
    for a in d["data"][0]["details"]:
        t = a["title"]
        spot = re.search(r"(?i)spot|/usdt", t); deriv = re.search(r"(?i)perpetual|futures", t)
        if re.search(r"(?i)\bto list\b", t) and spot:
            slug = (a.get("url") or "").rstrip("/").split("/")[-1] or f"{a.get('pTime')}:{t.lower()}"
            out.append({"key": "okx:" + slug, "ex": "OKX", "title": t, "ts": int(a["pTime"]) / 1000, "url": a.get("url"),
                        "sym": tick(t) or (re.search(r"list ([A-Z0-9]{2,12})/", t) or [None, None])[1]})
    return out

def upbit():
    d = get("https://api-manager.upbit.com/api/v1/announcements?os=web&page=1&per_page=20&category=trade")
    out = []
    for a in d["data"]["notices"]:
        t = a["title"]
        if re.search(r"(디지털 자산 추가|신규 거래지원)", t) and a.get("listed_at"):
            ts = datetime.fromisoformat(a["listed_at"])
            if ts.tzinfo is None: ts = ts.replace(tzinfo=JST)
            out.append({"key": "upbit:" + str(a["id"]), "ex": "Upbit", "title": t, "ts": ts.timestamp(),
                        "url": f"https://upbit.com/service_center/notice?id={a['id']}", "sym": tick(t)})
    return out

def bybit():
    d = get("https://api.bybit.com/v5/announcements/index?locale=en-US&type=new_crypto&limit=20")
    out = []
    for a in d["result"]["list"]:
        t = a["title"]; tags = " ".join(a.get("tags") or [])
        if "Spot Listings" in tags and re.search(r"(?i)listing|will list|to list", t) and not re.search(r"(?i)splash|perpetual", t):
            ts = (a.get("publishTime") or a.get("dateTimestamp") or 0) / 1000
            slug = (a.get("url") or "").rstrip("/").split("/")[-1] or t.lower()
            out.append({"key": "bybit:" + slug, "ex": "Bybit", "title": t, "ts": ts, "url": a.get("url"),
                        "sym": (re.search(r"\b([A-Z0-9]{2,12})USDT\b", t) or [None, None])[1]})
    return out

def send(text):
    d = urllib.parse.urlencode({"chat_id": os.environ["TG_CHAT"], "text": text, "disable_web_page_preview": "true"}).encode()
    urllib.request.urlopen(f"https://api.telegram.org/bot{os.environ['TG_TOKEN']}/sendMessage", d, timeout=20).read()

def save(seen):
    tmp = SEEN_F + ".tmp"; json.dump(seen, open(tmp, "w"), indent=0, sort_keys=True); os.replace(tmp, SEEN_F)

def main():
    try:
        seen = json.load(open(SEEN_F))
    except Exception:
        seen = {}
    seen.setdefault("keys", []); seen.setdefault("seeded", []); seen.setdefault("fails", {}); seen.setdefault("notified_down", [])
    seen.setdefault("pinged_syms", {})
    now = time.time(); report = {}
    # Upbit and Bybit geo-block GitHub's US runners (HTTP 403, confirmed 2026-09-26); enable via WATCH_SOURCES if run elsewhere
    enabled = os.environ.get("WATCH_SOURCES", "Binance,OKX").split(",")
    for name, fn in (("Binance", binance), ("OKX", okx), ("Upbit", upbit), ("Bybit", bybit)):
        if name not in enabled: continue
        try:
            items = fn()
        except Exception as e:
            seen["fails"][name] = seen["fails"].get(name, 0) + 1
            report[name] = f"error {type(e).__name__}"
            if seen["fails"][name] >= FAIL_N and name not in seen["notified_down"]:
                try:
                    send(f"X-scout watcher: {name} announcements unreachable from GitHub ({type(e).__name__}, {FAIL_N}+ runs). "
                         f"Other exchanges are still watched; no further notice until it recovers.")
                    seen["notified_down"].append(name)
                except Exception:
                    pass
            continue
        report[name] = f"ok {len(items)}"
        if seen["fails"].get(name, 0) and name in seen["notified_down"]:
            try:
                send(f"X-scout watcher: {name} announcements reachable again."); seen["notified_down"].remove(name)
            except Exception:
                pass
        seen["fails"][name] = 0
        if name not in seen["seeded"]:        # first successful response: seed silently
            seen["keys"] += [i["key"] for i in items if i["key"] not in seen["keys"]]
            seen["seeded"].append(name); continue
        for i in items:
            if i["key"] in seen["keys"]: continue
            if now - i["ts"] > 12 * 3600:      # stale item surfacing late: record, don't ping
                seen["keys"].append(i["key"]); continue
            sym = i.get("sym")
            last = seen["pinged_syms"].get(f"{name}:{sym}") if sym else None
            if last and now - last < 24 * 3600:   # e.g. Binance "Will List X" + "HODLer Airdrops X" for the same coin
                seen["keys"].append(i["key"]); continue
            age = max(0, int((now - i["ts"]) / 60))
            try:
                send(f"⚡ LISTING — {i['ex']}\n{i['title']}\nAnnounced {datetime.fromtimestamp(i['ts'], JST).strftime('%m-%d %H:%M')} JST "
                     f"({age} min ago)\n{i.get('url') or FALLBACK[name]}\nFast ping only, not verified — the X-scout run analyses it "
                     f"within 3h. Research signal, not advice.")
            except Exception as e:
                report[name] += f" (send failed: {type(e).__name__}; will retry)"
                continue                        # not marked seen -> retried next run
            seen["keys"].append(i["key"])
            if sym: seen["pinged_syms"][f"{name}:{sym}"] = now
    seen["keys"] = seen["keys"][-3000:]
    seen["pinged_syms"] = {k: v for k, v in seen["pinged_syms"].items() if now - v < 3 * 86400}
    save(seen)
    print(datetime.now(timezone.utc).isoformat(timespec="minutes"), json.dumps(report))

if __name__ == "__main__":
    main()
