#!/usr/bin/env python3
"""Fast listing watcher (runs on GitHub Actions every ~5 min). Pings Telegram the moment a high-impact exchange
listing appears. The 3-hourly X-scout run does the deep analysis; this only buys speed.

Sources (feeds), grouped for WATCH_SOURCES (comma list of groups or feeds, case-insensitive; default "all"):
  binance  Binance    spot listing / HODLer Airdrops announcements
  okx      OKX        spot listing announcements
  coinbase Coinbase   Advanced Trade public products: a new base asset appears (new_at)
           CoinbaseStatus  Coinbase Exchange status RSS "<PAIR> Markets Open"
  upbit    Upbit      KRW market set diff (api.upbit.com/v1/market/all)
           UpbitWarn  new investment warnings (투자유의) on KRW markets
  bithumb  Bithumb    KRW market set diff (api.bithumb.com/v1/market/all)
           BithumbNotice  listing notices (feed-api.bithumb.com, latest 5)
  opt-in only (geo-blocked on GitHub runners): bybit (Bybit), upbit_notice (UpbitNotice)

Each ping carries a price (exchange data or one CoinGecko call) and a "Base rate:" line from priors.json in the
x-scout repo, cached here as priors_cache.json (refreshed at most once a day; missing file -> "not measured yet").

State: seen.json (committed back by the workflow only when it changes). Each feed is seeded silently the first time it
responds. An item is marked seen only after its ping was sent. A feed failing 3 runs in a row triggers one notice; one
more notice when it recovers. The repo is public: stdout carries counts only.
Env: TG_TOKEN, TG_CHAT, WATCH_SOURCES, WATCH_SILENT=1 (dry run: print messages instead of sending; state still saved,
so use a copy)."""
import html as htmlmod, json, os, re, time, urllib.request, urllib.parse, urllib.error
from datetime import datetime, timezone, timedelta

UA = {"User-Agent": "Mozilla/5.0 x-scout-watcher", "Accept": "application/json"}
SEEN_F = "seen.json"
PRIORS_F = "priors_cache.json"
PRIORS_URL = "https://raw.githubusercontent.com/josephcheung2000-star/x-scout/main/priors.json"
JST = timezone(timedelta(hours=9))
FAIL_N = 3
STALE_S = 12 * 3600          # item older than this when first seen: record, don't ping
BURST = 5                    # more new items than this from one feed in one run -> one digest message
MAX_PRICE_LOOKUPS = 6        # CoinGecko calls per run
SILENT = os.environ.get("WATCH_SILENT") == "1"
GROUPS = {"binance": ["Binance"], "okx": ["OKX"], "coinbase": ["Coinbase", "CoinbaseStatus"],
          "upbit": ["Upbit", "UpbitWarn"], "bithumb": ["Bithumb", "BithumbNotice"]}
OPT_IN = {"bybit": ["Bybit"], "upbit_notice": ["UpbitNotice"]}
CATALYST = {"Binance": "binance_new_listing", "OKX": "okx_listing", "Coinbase": "coinbase_listing",
            "CoinbaseStatus": "coinbase_listing", "Upbit": "upbit_krw_listing", "UpbitNotice": "upbit_krw_listing",
            "Bithumb": "bithumb_krw_listing", "BithumbNotice": "bithumb_krw_listing", "UpbitWarn": "upbit_warning",
            "Bybit": "bybit_listing"}
EXCH = {"CoinbaseStatus": "Coinbase", "UpbitWarn": "Upbit", "UpbitNotice": "Upbit", "BithumbNotice": "Bithumb"}
FALLBACK = {"Binance": "https://www.binance.com/en/support/announcement/list/48",
            "OKX": "https://www.okx.com/help/section/announcements-new-listings",
            "Coinbase": "https://www.coinbase.com/advanced-trade", "CoinbaseStatus": "https://status.exchange.coinbase.com",
            "Upbit": "https://upbit.com/exchange", "UpbitWarn": "https://upbit.com/exchange",
            "UpbitNotice": "https://upbit.com/service_center/notice", "Bithumb": "https://www.bithumb.com/react/trade/order",
            "BithumbNotice": "https://feed.bithumb.com/notice", "Bybit": "https://announcements.bybit.com/en/?category=new_crypto"}


def fetch(url, timeout=20):
    with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=timeout) as r:
        return r.read()


def get(url, timeout=20):
    return json.loads(fetch(url, timeout))


def tick(t):
    m = re.search(r"\(([A-Z0-9]{2,12})\)", t)
    return m.group(1) if m else None


def item(key, title, ts, url, sym, **kw):
    d = {"key": key, "title": title, "ts": ts, "url": url, "sym": sym}
    d.update(kw)
    return d


# ---------------- announcement feeds (keys accumulate, capped) ----------------
def binance():
    d = get("https://www.binance.com/bapi/composite/v1/public/cms/article/list/query?type=1&catalogId=48&pageNo=1&pageSize=20")
    out = []
    for c in d["data"]["catalogs"]:
        if c["catalogId"] != 48: continue
        for a in c["articles"]:
            t = a["title"]
            if re.search(r"(?i)(binance will list|hodler airdrops)", t):
                out.append(item("binance:" + a["code"], t, a["releaseDate"] / 1000,
                                f"https://www.binance.com/en/support/announcement/{a['code']}", tick(t)))
    return out


def okx():
    d = get("https://www.okx.com/api/v5/support/announcements?annType=announcements-new-listings")
    out = []
    for a in d["data"][0]["details"]:
        t = a["title"]
        spot = re.search(r"(?i)spot|/usdt", t); deriv = re.search(r"(?i)perpetual|futures", t)
        if re.search(r"(?i)\bto list\b", t) and (re.search(r"(?i)spot", t) or (spot and not deriv)):
            slug = (a.get("url") or "").rstrip("/").split("/")[-1] or f"{a.get('pTime')}:{t.lower()}"
            out.append(item("okx:" + slug, t, int(a["pTime"]) / 1000, a.get("url"),
                            tick(t) or (re.search(r"list ([A-Z0-9]{2,12})/", t) or [None, None])[1]))
    return out


def coinbase_status():
    s = fetch("https://status.exchange.coinbase.com/history.rss").decode("utf-8", "replace")
    out = []
    for it in re.findall(r"<item>(.*?)</item>", s, re.S):
        f = lambda tag: htmlmod.unescape((re.search(rf"<{tag}>(.*?)</{tag}>", it, re.S) or [None, ""])[1].strip())
        t = f("title")
        m = re.match(r"\s*([A-Z0-9]{2,15})-([A-Z]{3,5}) Markets? Open", t, re.I)
        if not m: continue
        ts = datetime.strptime(f("pubDate"), "%a, %d %b %Y %H:%M:%S %z").timestamp()
        out.append(item("cbstatus:" + (f("guid") or t), t, ts, f("link") or None, m.group(1).upper()))
    return out


def bithumb_notice():
    out = []
    for n in get("https://feed-api.bithumb.com/v1/notices"):
        t = n["title"]
        if not re.search(r"마켓 추가|신규 거래지원|거래지원 개시|원화 마켓", t): continue
        ts = datetime.fromisoformat(n["published_at"]).replace(tzinfo=JST).timestamp()   # KST == JST offset
        out.append(item("bithumbnotice:" + n["pc_url"].rstrip("/").rsplit("/", 1)[1], t, ts, n["pc_url"], tick(t)))
    return out


def upbit_notice():
    d = get("https://api-manager.upbit.com/api/v1/announcements?os=web&page=1&per_page=20&category=trade")
    out = []
    for a in d["data"]["notices"]:
        t = a["title"]
        if re.search(r"(디지털 자산 추가|신규 거래지원)", t) and a.get("listed_at"):
            ts = datetime.fromisoformat(a["listed_at"])
            if ts.tzinfo is None: ts = ts.replace(tzinfo=JST)
            out.append(item("upbit:" + str(a["id"]), t, ts.timestamp(),
                            f"https://upbit.com/service_center/notice?id={a['id']}", tick(t)))
    return out


def bybit():
    d = get("https://api.bybit.com/v5/announcements/index?locale=en-US&type=new_crypto&limit=20")
    out = []
    for a in d["result"]["list"]:
        t = a["title"]; tags = " ".join(a.get("tags") or [])
        if "Spot Listings" in tags and re.search(r"(?i)listing|will list|to list", t) and not re.search(r"(?i)splash|perpetual", t):
            ts = (a.get("publishTime") or a.get("dateTimestamp") or 0) / 1000
            slug = (a.get("url") or "").rstrip("/").split("/")[-1] or t.lower()
            out.append(item("bybit:" + slug, t, ts, a.get("url"),
                            (re.search(r"\b([A-Z0-9]{2,12})USDT\b", t) or [None, None])[1]))
    return out


# ---------------- snapshot feeds (state = current set; vanished keys are dropped) ----------------
def coinbase():
    ps = get("https://api.coinbase.com/api/v3/brokerage/market/products?product_type=SPOT")["products"]
    by_base = {}
    for p in ps:
        b = p.get("base_currency_id") or p["product_id"].split("-")[0]
        by_base.setdefault(b, []).append(p)
    out = []
    for b, lst in by_base.items():
        p = next((x for x in lst if x.get("quote_currency_id") == "USD"), None) or \
            next((x for x in lst if x.get("quote_currency_id") == "USDC"), lst[0])
        stamps = [x["new_at"] for x in lst if x.get("new_at")]
        ts = min(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp() for s in stamps) if stamps else time.time()
        state = "trading" if p.get("status") == "online" and not p.get("trading_disabled") else (p.get("status") or "?")
        if p.get("limit_only"): state = "limit-only"
        if p.get("view_only"): state = "view-only"
        price, ch = p.get("price"), p.get("price_percentage_change_24h")
        out.append(item("coinbase:" + b, f"Coinbase lists {p.get('base_name') or b} ({b}) — {p['product_id']}, {state}",
                        min(ts, time.time()), f"https://www.coinbase.com/advanced-trade/spot/{p['product_id']}", b,
                        price=("USD" if p.get("quote_currency_id") in ("USD", "USDC") else p.get("quote_currency_id"),
                               float(price), float(ch) if ch not in (None, "") else None) if price not in (None, "") else None,
                        label="New on Coinbase since"))
    return out


def krw_markets(url, ex):
    ms = [m for m in get(url) if m["market"].startswith("KRW-")]
    base = "https://upbit.com/exchange?code=CRIX.UPBIT." if ex == "Upbit" else "https://www.bithumb.com/react/trade/order/"
    out = []
    for m in ms:
        sym = m["market"].split("-", 1)[1]
        u = base + (m["market"] if ex == "Upbit" else f"{sym}-KRW")
        out.append(item(f"{ex.lower()}:{m['market']}", f"{ex} adds KRW market: {m.get('english_name') or sym} ({sym})",
                        time.time(), u, sym, detected=True, raw=m))
    return out


def upbit():
    return krw_markets("https://api.upbit.com/v1/market/all?isDetails=true", "Upbit")


def bithumb():
    return krw_markets("https://api.bithumb.com/v1/market/all?isDetails=true", "Bithumb")


def upbit_warn():
    out = []
    for i in upbit():
        m = i["raw"]; ev = m.get("market_event") or {}
        flags = []
        if ev.get("warning") or m.get("market_warning") == "CAUTION": flags.append("WARNING")
        # caution sub-flags (GLOBAL_PRICE_DIFFERENCES, PRICE_FLUCTUATIONS, TRADING_VOLUME_SOARING, ...) toggle many times a day:
        # noise, never pinged. Only the real investment warning (투자유의, delisting risk) is.
        for f in flags:
            out.append(item(f"upbitwarn:{m['market']}:{f}", f"Upbit flag on {i['sym']}: {f}", time.time(), i["url"],
                            i["sym"], detected=True, flag=f))
    return out


FEEDS = {"Binance": (binance, False), "OKX": (okx, False), "Coinbase": (coinbase, True),
         "CoinbaseStatus": (coinbase_status, False), "Upbit": (upbit, True), "UpbitWarn": (upbit_warn, True),
         "Bithumb": (bithumb, True), "BithumbNotice": (bithumb_notice, False), "Bybit": (bybit, False),
         "UpbitNotice": (upbit_notice, False)}


# ---------------- priors / base rate ----------------
def load_priors(now):
    try:
        cache = json.load(open(PRIORS_F))
    except Exception:
        cache = {}
    if now - cache.get("checked_at", 0) < 86400:
        return cache, "cached"
    try:
        data = json.loads(fetch(PRIORS_URL, timeout=10))
        if not isinstance(data, dict): raise ValueError("priors not an object")
        cache = {"checked_at": int(now), "status": "ok", "priors": data}
    except urllib.error.HTTPError as e:
        if e.code == 404:            # not published yet: remember for a day, drop stale data
            cache = {"checked_at": int(now), "status": "404", "priors": None}
        else:                        # transient: keep old priors, retry in ~1h
            cache = {"checked_at": int(now) - 86400 + 3600, "status": f"http {e.code}", "priors": cache.get("priors")}
    except Exception as e:
        cache = {"checked_at": int(now) - 86400 + 3600, "status": type(e).__name__, "priors": cache.get("priors")}
    tmp = PRIORS_F + ".tmp"; json.dump(cache, open(tmp, "w"), indent=1, sort_keys=True); os.replace(tmp, PRIORS_F)
    return cache, "refreshed:" + cache["status"]


def _pick(d, *names):
    for n in names:
        if isinstance(d.get(n), (int, float)): return float(d[n])
    return None


def _pct(v):                          # accepts fractions (-0.18) or percents (-18)
    return v * 100 if abs(v) <= 1.5 else v


def _sgn(v):
    return ("−" if v < 0 else "+") + f"{abs(v):.0f}%"


def base_rate(priors, ctype):
    p = priors if isinstance(priors, dict) else {}
    for k in ("types", "catalysts", "priors"):
        if isinstance(p.get(k), dict): p = p[k]; break
    d = p.get(ctype)
    if not isinstance(d, dict): return "Base rate: not measured yet"
    n = _pick(d, "n", "count", "N")
    med = _pick(d, "median_excess_7d", "median_7d_excess", "med_excess_7d", "median_excess_7d_vs_btc", "median_excess")
    hit = _pick(d, "hit_rate", "hit", "hit_rate_7d", "win_rate")
    note = d.get("note") or d.get("summary") or d.get("verdict")
    if med is None and hit is None: return "Base rate: not measured yet"
    parts = []
    if med is not None: parts.append(f"7d median excess {_sgn(_pct(med))} vs BTC")
    if hit is not None: parts.append(f"hit {_pct(hit):.0f}%")
    s = f"Base rate{f' (n={int(n)})' if n else ''}: " + ", ".join(parts)
    return s + (f" — {note}" if isinstance(note, str) and note.strip() else "")


# ---------------- price ----------------
def fmt_price(cur, v):
    sign = "$" if cur == "USD" else ""
    txt = f"{v:,.2f}" if v >= 100 else f"{v:,.4f}" if v >= 1 else f"{v:.6g}"
    return f"{sign}{txt}" if sign else f"{txt} {cur}"


def price_line(i, budget):
    try:
        if i.get("price"):
            cur, v, ch = i["price"]
            return f"Price: {fmt_price(cur, v)}" + (f" ({ch:+.1f}% 24h)" if ch is not None else "")
        sym = i.get("sym")
        if not sym or budget[0] <= 0: return None
        budget[0] -= 1
        d = get("https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&symbols=" + urllib.parse.quote(sym.lower()), 10)
        d = [x for x in d if x.get("current_price") is not None]
        if not d: return None
        x = max(d, key=lambda r: r.get("market_cap") or 0)
        ch = x.get("price_change_percentage_24h")
        return f"Price: {fmt_price('USD', float(x['current_price']))}" + (f" ({ch:+.1f}% 24h)" if ch is not None else "") + \
               f" · CoinGecko: {x.get('name')}"
    except Exception:
        return None


# ---------------- messaging / state ----------------
def send(text):
    if SILENT:
        print("---- WOULD SEND ----\n" + text + "\n--------------------")
        return
    d = urllib.parse.urlencode({"chat_id": os.environ["TG_CHAT"], "text": text, "disable_web_page_preview": "true"}).encode()
    urllib.request.urlopen(f"https://api.telegram.org/bot{os.environ['TG_TOKEN']}/sendMessage", d, timeout=20).read()


def render(name, i, now, priors, budget):
    ex = EXCH.get(name, name)
    when = datetime.fromtimestamp(i["ts"], JST).strftime("%m-%d %H:%M")
    age = max(0, int((now - i["ts"]) / 60))
    head = "⚠️ UPBIT FLAG" if name == "UpbitWarn" else "⚡ LISTING"
    lines = [f"{head} — {ex}" + (f" · {i['sym']}" if i.get("sym") else ""), i["title"],
             (f"Detected {when} JST (API change)" if i.get("detected") else f"{i.get('label', 'Announced')} {when} JST ({age} min ago)")]
    p = price_line(i, budget)
    if p: lines.append(p)
    lines.append(base_rate(priors, CATALYST.get(name, name.lower())))
    lines.append(i.get("url") or FALLBACK[name])
    lines.append("Fast ping, unverified; X-scout analyses within 3h. Research signal, not advice.")
    return "\n".join(lines)


def render_digest(name, items, priors):
    ex = EXCH.get(name, name)
    syms = ", ".join(sorted({i.get("sym") or i["key"].split(":")[-1] for i in items}))[:600]
    return (f"⚡ {len(items)} new items — {ex} ({name})\n{syms}\n{base_rate(priors, CATALYST.get(name, name.lower()))}\n"
            f"{FALLBACK[name]}\nBurst digest (>{BURST} at once) — check the exchange before acting. Not advice.")


def save(seen):
    tmp = SEEN_F + ".tmp"; json.dump(seen, open(tmp, "w"), indent=0, sort_keys=True); os.replace(tmp, SEEN_F)


def enabled_feeds():
    raw = [s.strip().lower() for s in os.environ.get("WATCH_SOURCES", "all").split(",") if s.strip()]
    lookup = {**GROUPS, **OPT_IN, **{f.lower(): [f] for f in FEEDS}}
    out = []
    for s in raw or ["all"]:
        for f in (sum(GROUPS.values(), []) if s == "all" else lookup.get(s, [])):
            if f not in out: out.append(f)
    return out


def main():
    try:
        seen = json.load(open(SEEN_F))
    except Exception:
        seen = {}
    for k, v in (("seeded", []), ("fails", {}), ("notified_down", []), ("pinged_syms", {}), ("src", {})):
        seen.setdefault(k, v)
    if "keys" in seen:                         # migrate v1 flat key list into per-feed lists
        pref = {"binance": "Binance", "okx": "OKX", "upbit": "UpbitNotice", "bybit": "Bybit"}
        for k in seen.pop("keys"):
            f = pref.get(k.split(":", 1)[0])
            if f: seen["src"].setdefault(f, []).append(k)
        if "Upbit" in seen["seeded"]: seen["seeded"][seen["seeded"].index("Upbit")] = "UpbitNotice"
        if "Upbit" in seen["fails"]: seen["fails"]["UpbitNotice"] = seen["fails"].pop("Upbit")   # v1 "Upbit" = notices
        if "Upbit" in seen["notified_down"]: seen["notified_down"][seen["notified_down"].index("Upbit")] = "UpbitNotice"
    now = time.time(); report = {}; budget = [MAX_PRICE_LOOKUPS]
    cache, pstat = load_priors(now)
    priors = cache.get("priors")
    for name in enabled_feeds():
        fn, snapshot = FEEDS[name]
        prev = seen["src"].get(name, [])
        try:
            items = fn()
            if snapshot and name in seen["seeded"] and len(prev) >= 20 and len({i["key"] for i in items}) < 0.5 * len(prev):
                raise ValueError("snapshot shrank >50%")     # partial API answer: don't treat the rest as new later
        except Exception as e:
            seen["fails"][name] = min(seen["fails"].get(name, 0) + 1, FAIL_N)   # capped: no commit churn during outages
            report[name] = f"error {type(e).__name__}"
            if seen["fails"][name] >= FAIL_N and name not in seen["notified_down"]:
                try:
                    send(f"X-scout watcher: {name} feed unreachable from GitHub ({type(e).__name__}, {FAIL_N}+ runs). "
                         f"Other feeds are still watched; no further notice until it recovers.")
                    seen["notified_down"].append(name)
                except Exception:
                    pass
            continue
        if name in seen["notified_down"]:
            seen["notified_down"].remove(name)          # re-arm the outage notice even if this message fails
            try: send(f"X-scout watcher: {name} feed reachable again.")
            except Exception: pass
        seen["fails"][name] = 0
        cur = list(dict.fromkeys(i["key"] for i in items))
        if name not in seen["seeded"]:                  # first successful response: seed silently
            seen["src"][name] = cur if snapshot else list(dict.fromkeys(prev + cur))
            seen["seeded"].append(name)
            report[name] = f"seeded {len(cur)}"; continue
        known = set(prev); done = []; new = [i for i in items if i["key"] not in known]
        todo = []
        for i in new:
            sym = i.get("sym"); grp = EXCH.get(name, name)
            if now - i["ts"] > STALE_S: done.append(i["key"]); continue
            if name != "UpbitWarn" and sym:
                last = seen["pinged_syms"].get(f"{grp}:{sym}")
                if last and now - last < 24 * 3600: done.append(i["key"]); continue   # same coin, other feed/notice
            todo.append(i)
        sent = failed = 0
        if name == "UpbitWarn":                          # one message per market, all new flags together
            by = {}
            for i in todo: by.setdefault(i["sym"], []).append(i)
            todo = [dict(g[0], title=f"Upbit flag on {s}: " + ", ".join(x["flag"] for x in g), keys=[x["key"] for x in g])
                    for s, g in by.items()]
        if len(todo) > BURST:
            try:
                send(render_digest(name, todo, priors)); sent = 1
                done += sum((i.get("keys") or [i["key"]] for i in todo), [])
            except Exception:
                failed = len(todo)
        else:
            for i in todo:
                try:
                    send(render(name, i, now, priors, budget))
                except Exception:
                    failed += 1; continue           # not marked seen -> retried next run
                sent += 1; done += i.get("keys") or [i["key"]]
                if i.get("sym") and name != "UpbitWarn": seen["pinged_syms"][f"{EXCH.get(name, name)}:{i['sym']}"] = now
        if snapshot:
            pending = {k for i in todo for k in (i.get("keys") or [i["key"]])} - set(done)
            seen["src"][name] = [k for k in cur if k not in pending]
        else:
            seen["src"][name] = (prev + done)[-500:]
        report[name] = f"ok {len(items)} new {len(new)} sent {sent}" + (f" failed {failed}" if failed else "")
    seen["pinged_syms"] = {k: v for k, v in seen["pinged_syms"].items() if now - v < 3 * 86400}
    save(seen)
    print(datetime.now(timezone.utc).isoformat(timespec="minutes"), "priors", pstat, json.dumps(report))


if __name__ == "__main__":
    main()
