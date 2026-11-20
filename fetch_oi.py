"""Daily OCC open interest by series for a list of underlyings.

Source: The Options Clearing Corporation (marketdata.theocc.com), public data.
Writes data/<SYMBOL>/<asof>.csv  (product,expiry,strike,call_oi,put_oi)
and   data/summary/<asof>.json  (totals per symbol and per expiry).
<asof> is the trading day the OCC open interest refers to (end of that day).
Exit code 0 when something new was written or nothing to do, 1 on error.
"""
import csv, datetime as dt, json, os, sys, time, urllib.request, urllib.error
from collections import defaultdict
from zoneinfo import ZoneInfo

SYMBOLS = ["NDX", "SOXX", "NBIS", "SKHY", "NVDA", "MSTR"]
SERIES_URL = "https://marketdata.theocc.com/series-search?symbolType=U&symbol={s}"
DATE_URL = "https://marketdata.theocc.com/mdapi/open-interest?report_date={d}"
UA = "Mozilla/5.0 (compatible; daily-oi-archive/1.0)"
COLS = ["ProductSymbol", "year", "Month", "Day", "Integer", "Dec", "C/P", "Call", "Put", "Position Limit"]
CT = ZoneInfo("America/Chicago")


def get(url, tries=4):
    last = None
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=40) as r:
                return r.read()
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(5 * (i + 1))
    raise RuntimeError(f"fetch failed {url}: {last}")


def asof_date():
    today = dt.datetime.now(CT).date()
    body = json.loads(get(DATE_URL.format(d=today.strftime("%m/%d/%Y"))))
    ms = body["entity"]["lastBusDateOI"]["activityDate"]
    return dt.datetime.fromtimestamp(ms / 1000, CT).date()


def parse(text):
    lines = text.splitlines()
    h = next((i for i, l in enumerate(lines) if l.startswith("ProductSymbol")), None)
    if h is None:
        raise RuntimeError("no series table: " + text.strip()[:120])
    header = [f.strip() for f in lines[h].split("\t") if f.strip()]
    if header != COLS:
        raise RuntimeError(f"unexpected columns {header}")
    rows = []
    for l in lines[h + 1:]:
        f = [x.strip() for x in l.split("\t") if x.strip()]
        if not f:
            continue
        if len(f) != len(COLS):
            raise RuntimeError(f"bad line {l!r}")
        prod, y, m, d, whole, dec, _flags, c, p, _lim = f
        rows.append((prod, dt.date(int(y), int(m), int(d)).isoformat(), float(f"{int(whole)}.{dec}"), int(c), int(p)))
    return rows


def main():
    asof = asof_date().isoformat()
    os.makedirs("data/summary", exist_ok=True)
    summary_path = f"data/summary/{asof}.json"
    summary = json.load(open(summary_path)) if os.path.exists(summary_path) else {"asof": asof, "source": "OCC series-search", "symbols": {}}
    errors = []
    for s in SYMBOLS:
        path = f"data/{s}/{asof}.csv"
        if os.path.exists(path) and s in summary["symbols"]:
            print(f"{s}: {asof} already saved")
            continue
        try:
            rows = parse(get(SERIES_URL.format(s=s)).decode("utf-8", "replace"))
        except Exception as e:  # noqa: BLE001
            errors.append(f"{s}: {e}")
            continue
        if not rows or sum(r[3] + r[4] for r in rows) == 0:
            errors.append(f"{s}: empty")
            continue
        os.makedirs(f"data/{s}", exist_ok=True)
        rows.sort(key=lambda r: (r[0], r[1], r[2]))
        with open(path, "w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["product", "expiry", "strike", "call_oi", "put_oi"])
            for r in rows:
                w.writerow([r[0], r[1], f"{r[2]:g}", r[3], r[4]])
        by_exp = defaultdict(lambda: [0, 0])
        by_prod = defaultdict(lambda: [0, 0])
        for prod, e, _k, c, p in rows:
            by_exp[e][0] += c; by_exp[e][1] += p
            by_prod[prod][0] += c; by_prod[prod][1] += p
        summary["symbols"][s] = {
            "call_oi": sum(r[3] for r in rows), "put_oi": sum(r[4] for r in rows),
            "products": {k: {"call_oi": v[0], "put_oi": v[1]} for k, v in sorted(by_prod.items())},
            "expiries": {k: {"call_oi": v[0], "put_oi": v[1]} for k, v in sorted(by_exp.items())},
            "rows": len(rows), "fetched_utc": dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        print(f"{s}: {asof} saved {len(rows)} series")
    with open(summary_path, "w") as fh:
        json.dump(summary, fh, indent=1, sort_keys=True)
    with open("data/latest.json", "w") as fh:
        json.dump({"asof": asof, "symbols": sorted(summary["symbols"]), "errors": errors}, fh, indent=1)
    if errors:
        print("ERRORS:\n" + "\n".join(errors))
        sys.exit(1)


if __name__ == "__main__":
    main()
