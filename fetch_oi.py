"""Coherent OCC OI collection with immutable trading-day outputs.

This preserves the
existing CSV and summary JSON consumer schema while adding a raw evidence bundle.
"""
from __future__ import annotations

import csv
import argparse
import datetime as dt
import gzip
import hashlib
import io
import json
import os
import shutil
import tempfile
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from zoneinfo import ZoneInfo

SYMBOLS = ("NDX", "SOXX", "NBIS", "SKHY", "NVDA", "MSTR")
ROOT_URL = "https://marketdata.theocc.com"
DATE_PATH = "/mdapi/open-interest?report_date="
SERIES_PATH = "/series-search?symbolType=U&symbol="
HEADER = ["ProductSymbol", "year", "Month", "Day", "Integer", "Dec", "C/P", "Call", "Put", "Position Limit"]
CSV_HEADER = ["product", "expiry", "strike", "call_oi", "put_oi"]
CT = ZoneInfo("America/Chicago")
UA = "Mozilla/5.0 (compatible; daily-oi-archive/1.0)"


@dataclass(frozen=True)
class Marker:
    requested_utc: str
    received_utc: str
    requested_date: str
    asof: str
    raw: bytes


@dataclass(frozen=True)
class Observation:
    symbol: str
    fetched_utc: str
    url: str
    raw: bytes
    rows: tuple


class HoldExisting(Exception):
    """Same-day files exist; keep them immutable and classify them as legacy/held."""

    def __init__(self, asof, paths):
        self.asof, self.paths = asof, tuple(map(str, paths))
        super().__init__(f"same-day output exists; re-fetch/write blocked for {asof}: {', '.join(self.paths)}")


def _utc(now_fn):
    value = now_fn()
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("clock must return an aware datetime")
    return value.astimezone(dt.timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _aware_utc(value):
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("clock must return an aware datetime")
    return value.astimezone(dt.timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _get(url):
    last = None
    for attempt in range(4):
        try:
            request = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(request, timeout=40) as response:
                return response.read()
        except Exception as exc:
            last = exc
            if attempt < 3: time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"OCC fetch failed: {url}: {last}") from last


def _asof_from_marker(raw):
    ms = json.loads(raw)["entity"]["lastBusDateOI"]["activityDate"]
    return dt.datetime.fromtimestamp(ms / 1000, CT).date().isoformat()


def marker(fetch, now_fn):
    instant = now_fn()
    if instant.tzinfo is None or instant.utcoffset() is None:
        raise ValueError("clock must return an aware datetime")
    requested = instant.astimezone(CT).date().strftime("%m/%d/%Y")
    url = ROOT_URL + DATE_PATH + urllib.parse.quote(requested, safe="/")
    requested_utc = _aware_utc(instant)
    raw = fetch(url)
    received_utc = _utc(now_fn)
    return Marker(requested_utc, received_utc, requested, _asof_from_marker(raw), raw)


def parse_series(symbol, raw):
    """Strictly parse exact OCC decimal strikes and reject malformed/empty payloads."""
    lines = raw.decode("utf-8", "strict").splitlines()
    if not lines or lines[0].strip() != f"Series Search Results for {symbol}":
        raise ValueError(f"wrong OCC underlying for {symbol}")
    try:
        header_index = next(i for i, line in enumerate(lines) if line.startswith("ProductSymbol"))
    except StopIteration as exc:
        raise ValueError("OCC series table header missing") from exc
    reader = csv.reader(lines[header_index:], delimiter="\t")
    header = [x.strip() for x in next(reader)]
    while header and header[-1] == "": header.pop()
    if header != HEADER:
        raise ValueError(f"OCC columns changed: {header!r}")
    rows, seen = [], set()
    for line_no, parts in enumerate(reader, header_index + 2):
        if not parts or not any(x.strip() for x in parts):
            continue
        while parts and not parts[-1].strip(): parts.pop()
        # OCC's archived response has one unnamed separator column after ProductSymbol.
        # Accept exactly that published layout; arbitrary interior blanks stay errors.
        if len(parts) == len(HEADER) + 1 and not parts[1].strip():
            del parts[1]
        if len(parts) != len(HEADER):
            raise ValueError(f"bad OCC row {line_no}: expected {len(HEADER)} fields, got {len(parts)}")
        product, year, month, day, whole, fraction, _cp, call, put, _limit = [x.strip() for x in parts]
        try:
            expiry = dt.date(int(year), int(month), int(day)).isoformat()
            strike = Decimal(f"{int(whole)}.{fraction}")
            calls, puts = int(call), int(put)
        except (ValueError, InvalidOperation) as exc:
            raise ValueError(f"invalid OCC numeric/date field at line {line_no}") from exc
        if not product or not strike.is_finite() or strike < 0 or calls < 0 or puts < 0:
            raise ValueError(f"invalid OCC value at line {line_no}")
        key = (product, expiry, format(strike, "f"))
        if key in seen:
            raise ValueError(f"duplicate OCC series {key}")
        seen.add(key)
        rows.append((product, expiry, strike, calls, puts))
    rows.sort(key=lambda r: (r[0], r[1], r[2]))
    standard = [r for r in rows if r[0] == symbol]
    if not standard or sum(r[3] + r[4] for r in standard) == 0:
        raise ValueError(f"missing or zero-OI standard product {symbol}")
    return tuple(rows)


def _existing_for_day(root, asof, symbols=SYMBOLS):
    root = Path(root)
    candidates = [root / "data" / "summary" / f"{asof}.json", root / "data" / "quality" / f"{asof}.json"]
    for symbol in symbols:
        candidates += [root / "data" / symbol / f"{asof}.csv",
                       root / "data" / "raw" / asof / f"{symbol}.tsv.gz"]
    return [p for p in candidates if p.exists()]


def _summary(asof, observations):
    result = {"asof": asof, "source": "OCC series-search", "symbols": {}}
    for symbol, observation in observations.items():
        by_exp, by_prod = defaultdict(lambda: [0, 0]), defaultdict(lambda: [0, 0])
        for product, expiry, _strike, calls, puts in observation.rows:
            by_exp[expiry][0] += calls; by_exp[expiry][1] += puts
            by_prod[product][0] += calls; by_prod[product][1] += puts
        result["symbols"][symbol] = {
            "call_oi": sum(r[3] for r in observation.rows),
            "put_oi": sum(r[4] for r in observation.rows),
            "products": {k: {"call_oi": v[0], "put_oi": v[1]} for k, v in sorted(by_prod.items())},
            "expiries": {k: {"call_oi": v[0], "put_oi": v[1]} for k, v in sorted(by_exp.items())},
            "rows": len(observation.rows),
            "fetched_utc": observation.fetched_utc,
        }
    return result


def _csv_bytes(rows):
    stream = io.StringIO(newline="")
    writer = csv.writer(stream)
    writer.writerow(CSV_HEADER)
    writer.writerows((p, e, format(k, "f"), c, v) for p, e, k, c, v in rows)
    return stream.getvalue().encode("utf-8")


def _json_bytes(value):
    return (json.dumps(value, ensure_ascii=False, indent=1, sort_keys=True) + "\n").encode("utf-8")


def _gzip(raw):
    return gzip.compress(raw, mtime=0)


def _fsync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try: os.fsync(fd)
    finally: os.close(fd)


def _install_new(path, content):
    """Atomic exclusive install: link refuses to replace an existing same-day file."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(content); stream.flush(); os.fsync(stream.fileno())
        os.link(temp_name, path)  # atomic, fails if target already exists
        _fsync_dir(path.parent)
    finally:
        try: os.unlink(temp_name)
        except FileNotFoundError: pass


def _atomic_replace(path, content):
    """Atomic replace reserved for latest.json and the run manifest's final status."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(content); stream.flush(); os.fsync(stream.fileno())
        os.replace(temp_name, path)
        _fsync_dir(path.parent)
    finally:
        try: os.unlink(temp_name)
        except FileNotFoundError: pass


def audit_legacy(root, asof, symbols=SYMBOLS):
    """Check legacy CSV/summary arithmetic without claiming source authenticity."""
    root = Path(root)
    summary_path = root / "data" / "summary" / f"{asof}.json"
    if not summary_path.exists():
        return {s: {"status": "INCOMPLETE", "reason": "same-day summary missing"} for s in symbols}
    try:
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        if summary.get("asof") != asof:
            raise ValueError("summary asof mismatch")
    except Exception as exc:
        return {s: {"status": "INVALID", "reason": f"summary unreadable: {exc}"} for s in symbols}
    manifest_path = root / "data" / "quality" / f"{asof}.json"
    manifest = None
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception:
            return {s: {"status": "INVALID", "reason": "quality manifest unreadable"} for s in symbols}
    if manifest and manifest.get("status") != "complete":
        return {s: {"status": "INCOMPLETE_PUBLISH", "reason": "same-day quality manifest is not complete"} for s in symbols}
    if manifest:
        try:
            for key in ("marker_before", "marker_after"):
                marker_info=manifest[key]
                expected_path=Path('data/raw')/asof/(key.replace('_','-')+'.json.gz')
                if Path(marker_info["raw_path"])!=expected_path:raise ValueError('Unexpected marker evidence path')
                gz=(root/expected_path).read_bytes()
                if hashlib.sha256(gz).hexdigest()!=marker_info["raw_gzip_sha256"]:
                    raise ValueError(f"{key} gzip hash mismatch")
                raw=gzip.decompress(gz)
                if hashlib.sha256(raw).hexdigest()!=marker_info["sha256"] or _asof_from_marker(raw)!=asof:
                    raise ValueError(f"{key} marker content/hash mismatch")
        except Exception as exc:
            return {s: {"status": "INVALID", "reason": str(exc)} for s in symbols}
    result = {}
    for symbol in symbols:
        path = root / "data" / symbol / f"{asof}.csv"
        block = summary.get("symbols", {}).get(symbol)
        if not path.exists() or not isinstance(block, dict):
            result[symbol] = {"status": "INCOMPLETE", "reason": "CSV or symbol summary missing"}
            continue
        try:
            with path.open(encoding="utf-8", newline="") as stream:
                reader = csv.reader(stream)
                if next(reader) != CSV_HEADER:
                    raise ValueError("legacy CSV header mismatch")
                parsed, seen = [], set()
                for parts in reader:
                    if len(parts) != len(CSV_HEADER): raise ValueError("malformed legacy CSV row")
                    product, expiry, strike_s, call_s, put_s = parts
                    strike, call, put = Decimal(strike_s), int(call_s), int(put_s)
                    if not product or strike < 0 or call < 0 or put < 0: raise ValueError("negative/empty value")
                    key=(product,expiry,format(strike,"f"))
                    if key in seen: raise ValueError("duplicate legacy series")
                    seen.add(key); parsed.append((product,expiry,strike,call,put))
            calls=sum(r[3] for r in parsed); puts=sum(r[4] for r in parsed)
            if len(parsed)!=block.get("rows") or calls!=block.get("call_oi") or puts!=block.get("put_oi"):
                raise ValueError("CSV rows/totals disagree with summary")
            expected=_summary(asof,{symbol:Observation(symbol,"","",b"",tuple(parsed))})["symbols"][symbol]
            for field in ("products","expiries"):
                if block.get(field)!=expected[field]: raise ValueError(f"CSV {field} totals disagree with summary")
            if manifest and manifest.get("status")=="complete" and symbol in manifest.get("symbols",{}):
                entry=manifest["symbols"][symbol]
                expected_path=Path('data/raw')/asof/(symbol+'.tsv.gz')
                if Path(entry["raw_path"])!=expected_path:raise ValueError('Unexpected raw evidence path')
                raw_path=root/expected_path
                raw_gz=raw_path.read_bytes()
                raw=gzip.decompress(raw_gz)
                if hashlib.sha256(raw).hexdigest()!=entry["raw_sha256"]: raise ValueError("raw evidence hash mismatch")
                if hashlib.sha256(raw_gz).hexdigest()!=entry["raw_gzip_sha256"]: raise ValueError("raw gzip hash mismatch")
                source_rows=parse_series(symbol,raw)
                csv_rows=tuple(parsed)
                if source_rows!=csv_rows: raise ValueError("raw source rows differ from published CSV")
                csv_raw=path.read_bytes()
                if hashlib.sha256(csv_raw).hexdigest()!=entry["csv_sha256"]: raise ValueError("CSV hash mismatch")
                result[symbol]={"status":"BUNDLE_VERIFIED","reason":"raw hash, parsed rows, CSV, and summary agree"}
            else:
                result[symbol]={"status":"LEGACY_STRUCTURALLY_CONSISTENT_UNVERIFIED","reason":"CSV and summary arithmetic agree; raw response/receipt/marker unavailable"}
        except Exception as exc:
            result[symbol]={"status":"INVALID","reason":str(exc)}
    return result


def collect_and_publish(root, *, fetch=_get, now_fn=lambda: dt.datetime.now(dt.timezone.utc), symbols=SYMBOLS):
    """Fetch/validate all missing data first, then publish no-overwrite daily artifacts."""
    root = Path(root)
    before = marker(fetch, now_fn)
    existing = _existing_for_day(root, before.asof, symbols)
    if existing:
        audit=audit_legacy(root,before.asof,symbols)
        if all(audit[s]["status"]=="BUNDLE_VERIFIED" for s in symbols):
            latest_path=root/"data"/"latest.json"
            latest=json.loads(latest_path.read_text(encoding="utf-8")) if latest_path.exists() else {}
            if latest.get("asof")==before.asof and set(latest.get("symbols",[]))==set(symbols) and latest.get("errors")==[]:
                summary=json.loads((root/"data"/"summary"/f"{before.asof}.json").read_text(encoding="utf-8"))
                manifest=json.loads((root/"data"/"quality"/f"{before.asof}.json").read_text(encoding="utf-8"))
                manifest["run_disposition"]="ALREADY_VERIFIED_SKIP"
                return summary,manifest
        raise HoldExisting(before.asof, existing)

    observations = {}
    for symbol in symbols:
        url = ROOT_URL + SERIES_PATH + urllib.parse.quote(symbol, safe="")
        raw = fetch(url)
        observations[symbol] = Observation(symbol, _utc(now_fn), url, raw, parse_series(symbol, raw))
    after = marker(fetch, now_fn)
    if before.asof != after.asof:
        raise ValueError(f"OCC basis changed during collection: {before.asof} -> {after.asof}")
    if set(observations) != set(symbols):
        raise ValueError("incomplete symbol set")

    data_root = root / "data"
    quality_dir = data_root / "quality"
    raw_dir = data_root / "raw" / before.asof
    manifest_path = quality_dir / f"{before.asof}.json"
    summary_path = data_root / "summary" / f"{before.asof}.json"
    latest_path = data_root / "latest.json"

    # Recheck after the network work to close the common concurrent-writer race.
    existing = _existing_for_day(root, before.asof, symbols)
    if existing:
        raise HoldExisting(before.asof, existing)

    manifest = {
        "asof": before.asof,
        "status": "publishing",
        "started_utc": before.requested_utc,
        "marker_before": {"requested_date": before.requested_date, "requested_utc": before.requested_utc,
                          "received_utc": before.received_utc,
                          "asof": before.asof, "sha256": hashlib.sha256(before.raw).hexdigest()},
        "marker_after": {"requested_date": after.requested_date, "requested_utc": after.requested_utc,
                         "received_utc": after.received_utc,
                         "asof": after.asof, "sha256": hashlib.sha256(after.raw).hexdigest()},
        "symbols": {},
    }
    _install_new(manifest_path, _json_bytes(manifest))
    try:
        for symbol, observation in observations.items():
            gz = _gzip(observation.raw)
            raw_path = raw_dir / f"{symbol}.tsv.gz"
            _install_new(raw_path, gz)
            csv_raw = _csv_bytes(observation.rows)
            csv_path = data_root / symbol / f"{before.asof}.csv"
            _install_new(csv_path, csv_raw)
            manifest["symbols"][symbol] = {
                "url": observation.url,
                "fetched_utc": observation.fetched_utc,
                "rows": len(observation.rows),
                "raw_sha256": hashlib.sha256(observation.raw).hexdigest(),
                "raw_gzip_sha256": hashlib.sha256(gz).hexdigest(),
                "raw_path": str(raw_path.relative_to(root)),
                "csv_sha256": hashlib.sha256(csv_raw).hexdigest(),
                "csv_path": str(csv_path.relative_to(root)),
            }
            _atomic_replace(manifest_path, _json_bytes(manifest))
        # Marker bodies are retained too, not only their parsed date/hash.
        for label, raw in (("marker-before", before.raw), ("marker-after", after.raw)):
            gz = _gzip(raw)
            p = raw_dir / f"{label}.json.gz"
            _install_new(p, gz)
            manifest[label.replace("-", "_")]["raw_path"] = str(p.relative_to(root))
            manifest[label.replace("-", "_")]["raw_gzip_sha256"] = hashlib.sha256(gz).hexdigest()

        summary = _summary(before.asof, observations)
        _install_new(summary_path, _json_bytes(summary))
        _atomic_replace(latest_path, _json_bytes({"asof": before.asof, "symbols": sorted(observations), "errors": []}))
        manifest["status"] = "complete"
        manifest["completed_utc"] = _utc(now_fn)
        _atomic_replace(manifest_path, _json_bytes(manifest))
    except Exception:
        # Leave the durable status at "publishing". Same-day reruns are blocked for audit.
        raise
    return summary, manifest


def main(argv=None):
    parser = argparse.ArgumentParser(description="Collect and archive one OCC OI snapshot")
    parser.add_argument("--root", type=Path, default=Path("."), help="collector root; defaults to workflow working directory")
    args = parser.parse_args(argv)
    summary, manifest = collect_and_publish(args.root)
    print(json.dumps({"asof": summary["asof"], "status": manifest["status"],
                      "disposition": manifest.get("run_disposition", "PUBLISHED")}, sort_keys=True))


if __name__ == "__main__":
    main()
