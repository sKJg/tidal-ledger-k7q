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
import uuid
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


class RollbackBlocked(RuntimeError):
    """Ownership or content checks failed; do not remove possibly external files."""


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


def _install_new(path, content, *, on_link=None):
    """Atomic exclusive install: link refuses to replace an existing same-day file."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(content); stream.flush(); os.fsync(stream.fileno())
        os.link(temp_name, path)  # atomic, fails if target already exists
        if on_link is not None:
            on_link(path)
        _fsync_dir(path.parent)
    finally:
        try: os.unlink(temp_name)
        except FileNotFoundError: pass


def _atomic_replace(path, content, *, on_replace=None):
    """Atomic replace reserved for latest.json and the run manifest's final status."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(content); stream.flush(); os.fsync(stream.fileno())
        os.replace(temp_name, path)
        if on_replace is not None:
            on_replace(path)
        _fsync_dir(path.parent)
    finally:
        try: os.unlink(temp_name)
        except FileNotFoundError: pass


def _sha256(raw):
    return hashlib.sha256(raw).hexdigest()


def _owned_file_state(path, expected_sha256):
    path = Path(path)
    if path.is_symlink():
        return "changed"
    if not path.exists():
        return "absent"
    if not path.is_file():
        return "changed"
    return "owned" if _sha256(path.read_bytes()) == expected_sha256 else "changed"


def _write_owned_manifest(path, manifest, transaction_id, *, on_replace=None):
    path = Path(path)
    if path.is_symlink() or not path.is_file():
        raise RollbackBlocked("transaction manifest is not a regular owned file")
    current = json.loads(path.read_text(encoding="utf-8"))
    if current.get("transaction_id") != transaction_id:
        raise RollbackBlocked("transaction manifest ownership changed; recovery is held")
    content = _json_bytes(manifest)
    _atomic_replace(path, content, on_replace=on_replace)


def _safe_transaction_path(root, relative):
    root = Path(root).resolve()
    relative = Path(relative)
    if relative.is_absolute() or ".." in relative.parts:
        raise RollbackBlocked(f"unsafe transaction path: {relative}")
    target = root / relative
    try:
        target.resolve(strict=False).relative_to(root)
    except ValueError as exc:
        raise RollbackBlocked(f"transaction path escapes data root: {relative}") from exc
    return target


def _rollback_transaction(root, *, manifest_path, transaction_id,
                          created, claim_hashes, latest_path, latest_new, latest_before, latest_replaced):
    """Undo only link/replace operations this run recorded and whose bytes still match."""
    root = Path(root)
    manifest_path = _safe_transaction_path(root, Path(manifest_path).relative_to(root))
    if not manifest_path.is_file() or manifest_path.is_symlink():
        raise RollbackBlocked("transaction claim disappeared or changed; outputs are held")
    try:
        disk_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RollbackBlocked(f"transaction claim unreadable; outputs are held: {exc}") from exc
    if disk_manifest.get("transaction_id") != transaction_id:
        raise RollbackBlocked("transaction claim ownership changed; outputs are held")
    if _sha256(manifest_path.read_bytes()) not in claim_hashes:
        raise RollbackBlocked("transaction claim bytes changed outside this execution; outputs are held")

    claimed = {}
    try:
        for entry in disk_manifest["planned_artifacts"]:
            rel = Path(entry["path"])
            if rel.is_absolute() or ".." in rel.parts:
                raise ValueError(f"unsafe claimed path: {rel}")
            claimed[rel.as_posix()] = entry["sha256"]
        if disk_manifest.get("latest_after_sha256") != _sha256(latest_new):
            raise ValueError("latest target hash differs from exclusive claim")
        if disk_manifest.get("latest_before") != {
            "exists": latest_before is not None,
            "sha256": _sha256(latest_before) if latest_before is not None else None,
        }:
            raise ValueError("prior latest snapshot differs from exclusive claim")
        for rel, digest in created.items():
            if claimed.get(rel) != digest:
                raise ValueError(f"created artifact is not covered by claim: {rel}")
    except Exception as exc:
        raise RollbackBlocked(f"transaction claim does not authorize cleanup: {exc}") from exc

    mismatches = []
    for rel, digest in created.items():
        target = _safe_transaction_path(root, rel)
        if _owned_file_state(target, digest) == "changed":
            mismatches.append(rel)
    if latest_replaced:
        latest_path = _safe_transaction_path(root, Path(latest_path).relative_to(root))
        state = _owned_file_state(latest_path, _sha256(latest_new))
        if state != "owned":
            mismatches.append(str(Path(latest_path).relative_to(root)))
    if mismatches:
        raise RollbackBlocked("artifact ownership/hash changed; refusing cleanup: " + ", ".join(mismatches))

    # Restore latest only when this run replaced it and it still contains our exact bytes.
    if latest_replaced:
        if latest_before is None:
            if _owned_file_state(latest_path, _sha256(latest_new)) != "owned":
                raise RollbackBlocked("latest.json changed before rollback; refusing removal")
            Path(latest_path).unlink()
            _fsync_dir(Path(latest_path).parent)
        else:
            if _owned_file_state(latest_path, _sha256(latest_new)) != "owned":
                raise RollbackBlocked("latest.json changed before restoration; refusing overwrite")
            _atomic_replace(latest_path, latest_before)

    # Remove only paths whose exclusive link succeeded during this process.
    for rel in reversed(tuple(created)):
        target = _safe_transaction_path(root, rel)
        state = _owned_file_state(target, created[rel])
        if state == "absent":
            continue
        if state != "owned":
            raise RollbackBlocked(f"artifact changed before unlink; refusing cleanup: {rel}")
        target.unlink()
        _fsync_dir(target.parent)

    # Remove our claim last so a new same-day run cannot overlap cleanup.
    current = json.loads(manifest_path.read_text(encoding="utf-8"))
    if current.get("transaction_id") != transaction_id:
        raise RollbackBlocked("transaction claim changed before final cleanup")
    if _sha256(manifest_path.read_bytes()) not in claim_hashes:
        raise RollbackBlocked("transaction claim changed before final cleanup")
    manifest_path.unlink()
    _fsync_dir(manifest_path.parent)


def _mark_rollback_blocked(root, manifest_path, transaction_id, reason):
    try:
        manifest_path = _safe_transaction_path(root, Path(manifest_path).relative_to(root))
        current = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
        if current.get("transaction_id") != transaction_id:
            return
        current["status"] = "rollback_blocked"
        current["rollback_error"] = str(reason)
        _atomic_replace(manifest_path, _json_bytes(current))
    except Exception:
        # Keep the existing claim if even the explanatory update cannot be persisted.
        return


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


def _publish_verified_pointer(root, bundle_root, asof, now_fn, symbols=SYMBOLS):
    """Publish a pointer only after the target bundle passes full source verification."""
    root=Path(root); bundle_root=Path(bundle_root)
    audit=audit_legacy(bundle_root,asof,symbols)
    if any(audit[s]["status"]!="BUNDLE_VERIFIED" for s in symbols):
        raise HoldExisting(asof,[bundle_root/"data"/"quality"/f"{asof}.json"])
    quality=bundle_root/"data"/"quality"/f"{asof}.json"
    summary=bundle_root/"data"/"summary"/f"{asof}.json"
    latest=bundle_root/"data"/"latest.json"
    try:
        manifest=json.loads(quality.read_text(encoding="utf-8"))
        summary_obj=json.loads(summary.read_text(encoding="utf-8"))
        latest_obj=json.loads(latest.read_text(encoding="utf-8"))
        if manifest.get("asof")!=asof or manifest.get("status")!="complete": raise ValueError("quality manifest is not complete for basis date")
        if summary_obj.get("asof")!=asof or set(summary_obj.get("symbols",{}))!=set(symbols): raise ValueError("summary does not match bundle date/symbols")
        if latest_obj.get("asof")!=asof or set(latest_obj.get("symbols",[]))!=set(symbols) or latest_obj.get("errors")!=[]: raise ValueError("bundle latest pointer is inconsistent")
    except Exception as exc:
        raise HoldExisting(asof,[quality,summary,latest]) from exc
    rel=str(bundle_root.relative_to(root)) if bundle_root!=root else "."
    quality_raw=quality.read_bytes()
    ready_utc=manifest.get("completed_utc")
    if not ready_utc: raise HoldExisting(asof,[quality])
    pointer={"asof":asof,"bundle_root":rel,"completed_utc":ready_utc,"quality_sha256":_sha256(quality_raw),"source":"OCC"}
    path=root/"data"/"verified-latest.json"
    if path.is_symlink() or (path.exists() and not path.is_file()): raise HoldExisting(asof,[path])
    if path.exists():
        try: current=json.loads(path.read_text(encoding="utf-8"))
        except Exception: current=None
        if current==pointer: return current
    _atomic_replace(path,_json_bytes(pointer))
    return pointer


def collect_and_publish(root, *, fetch=_get, now_fn=lambda: dt.datetime.now(dt.timezone.utc), symbols=SYMBOLS, allow_recheck=True, _expected_asof=None, _publish_pointer=True):
    """Fetch/validate all missing data first, then publish no-overwrite daily artifacts."""
    root = Path(root)
    before = marker(fetch, now_fn)
    if _expected_asof and before.asof!=_expected_asof:
        raise ValueError(f"expected OCC basis {_expected_asof}, got {before.asof}")
    existing = _existing_for_day(root, before.asof, symbols)
    if existing:
        audit=audit_legacy(root,before.asof,symbols)
        if all(audit[s]["status"]=="BUNDLE_VERIFIED" for s in symbols):
            latest_path=root/"data"/"latest.json"
            latest=json.loads(latest_path.read_text(encoding="utf-8")) if latest_path.exists() else {}
            if latest.get("asof")==before.asof and set(latest.get("symbols",[]))==set(symbols) and latest.get("errors")==[]:
                summary=json.loads((root/"data"/"summary"/f"{before.asof}.json").read_text(encoding="utf-8"))
                manifest=json.loads((root/"data"/"quality"/f"{before.asof}.json").read_text(encoding="utf-8"))
                if _publish_pointer: _publish_verified_pointer(root,root,before.asof,now_fn,symbols)
                manifest["run_disposition"]="ALREADY_VERIFIED_SKIP"
                return summary,manifest
        if allow_recheck:
            recheck_root=root/"data"/"rechecks"/before.asof
            recheck_summary,recheck_manifest=collect_and_publish(recheck_root,fetch=fetch,now_fn=now_fn,symbols=symbols,allow_recheck=False,_expected_asof=before.asof,_publish_pointer=False)
            _publish_verified_pointer(root,recheck_root,before.asof,now_fn,symbols)
            recheck_manifest=dict(recheck_manifest)
            if recheck_manifest.get("run_disposition")!="ALREADY_VERIFIED_SKIP": recheck_manifest["run_disposition"]="LEGACY_RECHECK_ARCHIVED"
            return recheck_summary,recheck_manifest
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

    transaction_id = uuid.uuid4().hex
    planned = {}
    symbol_manifest = {}
    for symbol, observation in observations.items():
        gz = _gzip(observation.raw)
        raw_path = raw_dir / f"{symbol}.tsv.gz"
        csv_raw = _csv_bytes(observation.rows)
        csv_path = data_root / symbol / f"{before.asof}.csv"
        planned[raw_path] = gz
        planned[csv_path] = csv_raw
        symbol_manifest[symbol] = {
            "url": observation.url,
            "fetched_utc": observation.fetched_utc,
            "rows": len(observation.rows),
            "raw_sha256": _sha256(observation.raw),
            "raw_gzip_sha256": _sha256(gz),
            "raw_path": str(raw_path.relative_to(root)),
            "csv_sha256": _sha256(csv_raw),
            "csv_path": str(csv_path.relative_to(root)),
        }

    marker_hashes = {}
    for label, marker_observation in (("marker-before", before), ("marker-after", after)):
        gz = _gzip(marker_observation.raw)
        marker_path = raw_dir / f"{label}.json.gz"
        planned[marker_path] = gz
        marker_hashes[label.replace("-", "_")] = {
            "requested_date": marker_observation.requested_date,
            "requested_utc": marker_observation.requested_utc,
            "received_utc": marker_observation.received_utc,
            "asof": marker_observation.asof,
            "sha256": _sha256(marker_observation.raw),
            "raw_path": str(marker_path.relative_to(root)),
            "raw_gzip_sha256": _sha256(gz),
        }

    summary = _summary(before.asof, observations)
    summary_raw = _json_bytes(summary)
    planned[summary_path] = summary_raw
    latest_raw = _json_bytes({"asof": before.asof, "symbols": sorted(observations), "errors": []})
    if latest_path.is_symlink() or (latest_path.exists() and not latest_path.is_file()):
        raise HoldExisting(before.asof, [latest_path])
    latest_before = latest_path.read_bytes() if latest_path.exists() else None
    latest_before_sha = _sha256(latest_before) if latest_before is not None else None

    manifest = {
        "asof": before.asof,
        "status": "publishing",
        "transaction_id": transaction_id,
        "started_utc": before.requested_utc,
        **marker_hashes,
        "symbols": symbol_manifest,
        "planned_artifacts": [
            {"path": str(path.relative_to(root)), "sha256": _sha256(content)}
            for path, content in planned.items()
        ],
        "latest_before": {"exists": latest_before is not None, "sha256": latest_before_sha},
        "latest_after_sha256": _sha256(latest_raw),
    }
    claim_installed = False
    claim_hashes = set()
    created = {}
    latest_replaced = False

    def record_created(path, content):
        rel = str(Path(path).relative_to(root))
        created[rel] = _sha256(content)

    claim_raw = _json_bytes(manifest)

    def claim_linked(_path):
        nonlocal claim_installed
        claim_installed = True
        claim_hashes.add(_sha256(claim_raw))

    try:
        _install_new(manifest_path, claim_raw, on_link=claim_linked)
        for path, content in planned.items():
            _install_new(path, content, on_link=lambda installed, payload=content: record_created(installed, payload))

        # Do not replace latest if another actor changed it since the claim snapshot.
        current_latest = latest_path.read_bytes() if latest_path.exists() and latest_path.is_file() else None
        if current_latest != latest_before:
            raise RollbackBlocked("latest.json changed after transaction claim; publication held")
        def latest_linked(_path):
            nonlocal latest_replaced
            latest_replaced = True
        _atomic_replace(latest_path, latest_raw, on_replace=latest_linked)

        manifest["status"] = "complete"
        manifest["completed_utc"] = _utc(now_fn)
        completed_raw = _json_bytes(manifest)
        _write_owned_manifest(
            manifest_path, manifest, transaction_id,
            on_replace=lambda _path: claim_hashes.add(_sha256(completed_raw)),
        )
    except Exception as original_error:
        if not claim_installed:
            raise
        try:
            _rollback_transaction(
                root,
                manifest_path=manifest_path,
                transaction_id=transaction_id,
                created=created,
                claim_hashes=claim_hashes,
                latest_path=latest_path,
                latest_new=latest_raw,
                latest_before=latest_before,
                latest_replaced=latest_replaced,
            )
        except Exception as rollback_error:
            _mark_rollback_blocked(root, manifest_path, transaction_id, rollback_error)
            raise RollbackBlocked(
                f"publication failed ({original_error}); rollback could not prove ownership "
                f"and same-day output remains held: {rollback_error}"
            ) from original_error
        raise
    if _publish_pointer: _publish_verified_pointer(root,root,before.asof,now_fn,symbols)
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
