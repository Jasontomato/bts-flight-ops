"""
Download monthly BTS "Reporting Carrier On-Time Performance" files, verify
them, record a manifest, and optionally upload them to S3.

Examples
    # most recent 12 months that BTS has published
    python ingestion/download_bts.py --months 12

    # an explicit range, then upload to S3
    python ingestion/download_bts.py --start 2025-09 --end 2026-08 --s3-bucket my-bucket

Re-running is safe: a month whose file is already downloaded and passed
validation is skipped.
"""
from __future__ import annotations

import argparse
import calendar
import csv
import hashlib
import io
import json
import logging
import statistics
import time
import zipfile
from datetime import date
from pathlib import Path

URL = ("https://transtats.bts.gov/PREZIP/"
       "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{y}_{m}.zip")
MAX_RETRIES = 5
BACKOFF_SECONDS = 5          # doubles after each failed attempt
TIMEOUT = (10, 300)          # (connect, read) seconds
LOOKBACK_MONTHS = 12         # how far back to search for published months
EXPECTED_COLUMNS = Path(__file__).parent / "expected_columns.json"  # schema baseline for rule 1
VOLUME_THRESHOLD = 0.15      # rule 2: max deviation from the median; calibrate after the first run

log = logging.getLogger("download_bts")


# ---------------------------------------------------------------- months
def month_range(start: str, end: str) -> list[tuple[int, int]]:
    """'2025-09', '2026-08' -> [(2025, 9), ..., (2026, 8)]"""
    y, m = map(int, start.split("-"))
    ey, em = map(int, end.split("-"))
    out = []
    while (y, m) <= (ey, em):
        out.append((y, m))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def months_back_from_today(n: int) -> list[tuple[int, int]]:
    """Last n calendar months before the current one, newest first."""
    y, m = date.today().year, date.today().month
    out = []
    for _ in range(n):
        y, m = (y - 1, 12) if m == 1 else (y, m - 1)
        out.append((y, m))
    return out


# -------------------------------------------------------------- download
def download(url: str, dest: Path) -> bool:
    """Download url to dest. Returns False if BTS has not published the file.

    Writes to a .part file first so an interrupted download never looks
    complete, retries network errors with exponential backoff, and checks
    the byte count against Content-Length.
    """
    import requests  # imported here so the inspection code runs without it

    part = dest.with_suffix(dest.suffix + ".part")
    wait = BACKOFF_SECONDS
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with requests.get(url, stream=True, timeout=TIMEOUT) as r:
                if r.status_code == 404:
                    return False
                r.raise_for_status()
                written = 0
                with open(part, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        f.write(chunk)
                        written += len(chunk)
                expected = r.headers.get("Content-Length")
                if expected is not None and int(expected) != written:
                    raise IOError(f"truncated: got {written} of {expected} bytes")
            with open(part, "rb") as f:
                if f.read(2) != b"PK":   # not a zip: BTS returned a page instead
                    part.unlink()
                    return False
            part.replace(dest)
            return True
        except Exception as exc:  # network errors, truncation, HTTP 5xx
            log.warning("attempt %d/%d failed for %s: %s", attempt, MAX_RETRIES, url, exc)
            if part.exists():
                part.unlink()
            if attempt == MAX_RETRIES:
                raise
            time.sleep(wait)
            wait *= 2
    return False


# --------------------------------------------------------------- inspect
def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def inspect_zip(path: Path) -> dict:
    """Facts about a downloaded file, used by validate_month()."""
    with zipfile.ZipFile(path) as z:
        bad = z.testzip()
        if bad is not None:
            raise zipfile.BadZipFile(f"corrupt member {bad}")
        csvs = [n for n in z.namelist() if n.lower().endswith(".csv")]
        if len(csvs) != 1:
            raise ValueError(f"expected 1 CSV in zip, found {len(csvs)}: {csvs}")
        with z.open(csvs[0]) as raw:
            reader = csv.reader(io.TextIOWrapper(raw, encoding="utf-8", newline=""))
            header = next(reader)
            iy, im = header.index("Year"), header.index("Month")
            idate = header.index("FlightDate")
            rows, year_months, dates = 0, set(), set()
            for row in reader:
                rows += 1
                year_months.add((int(row[iy]), int(row[im])))
                dates.add(row[idate])
    return {
        "file": path.name,
        "zip_bytes": path.stat().st_size,
        "sha256": sha256(path),
        "csv_name": csvs[0],
        "row_count": rows,
        "column_count": len(header),
        "columns": header,
        "year_months_in_file": sorted(year_months),
        "flight_dates": sorted(dates),          # e.g. ["2025-12-01", ...]
    }


# -------------------------------------------------------------- validate
def validate_month(stats: dict, year: int, month: int) -> list[str]:
    """Return a list of failure messages; an empty list means the month passes.

    TODO (you): implement your validation rules here.
    Available in `stats`: zip_bytes, row_count, column_count, columns,
    year_months_in_file, flight_dates, csv_name, sha256.
    Tip: calendar.monthrange(year, month)[1] gives the number of days in a month.
    """
    failures: list[str] = []
    expected = json.loads(EXPECTED_COLUMNS.read_text())
    if stats["columns"] != expected:
        failures.append(
            "rule 1: column names differ from the expected schema"
        )

    if stats["year_months_in_file"] != [(year, month)]:
        failures.append(
            "rule 3: file does not contain only the requested year and month"
        )

    if calendar.monthrange(year, month)[1] != len(stats["flight_dates"]):
        failures.append(
            "rule 5: flight dates do not cover every day of the requested month"
        )

    return failures


def volume_warnings(manifest: dict) -> dict[str, list[str]]:
    """Batch-level check (rule 2). Runs after all months are processed.

    Unlike validate_month(), this check compares months with each other, so it
    needs history and only produces warnings: it never blocks a month.
    Return {"YYYY-MM": [warning messages]} for months that look unusual.

    TODO (you): implement rule 2 here.
    Each manifest entry has row_count and status; the key is "YYYY-MM".
    Only compare months whose status is "passed".
    """
    # 
    

    warnings: dict[str, list[str]] = {}
    passed = {
        key: entry
        for key, entry in manifest.items()
        if entry.get("status") == "passed"
    }

    if len(passed)<4:
        return warnings

    dailyRowPerMonth:dict[str, float] = {}
    for key, entry in passed.items():
        # Get each months' daily row count
        year_text, month_text = key.split("-")
        year, month = int(year_text), int(month_text)
        daysOfmonth = calendar.monthrange(year,month)[1]
        rowPerday = entry.get("row_count") / daysOfmonth
        dailyRowPerMonth.update({key:rowPerday})
   
    for key,value in dailyRowPerMonth.items():
        copied = dailyRowPerMonth.copy()
        copied.pop(key)
        otherMonthdailyRow = statistics.median(copied.values())
        pct = (value-otherMonthdailyRow)/otherMonthdailyRow
        if abs(pct) > VOLUME_THRESHOLD:
            warnings[key] = [
                "rule 2: daily row count of {key} is {pct:+.2%} different from other months"
            ]
    return warnings


# ------------------------------------------------------------------- S3
def upload_s3(path: Path, bucket: str, prefix: str, year: int, month: int) -> str:
    import boto3
    key = f"{prefix}/year={year}/month={month:02d}/{path.name}"
    boto3.client("s3").upload_file(str(path), bucket, key)
    return f"s3://{bucket}/{key}"


# ------------------------------------------------------------- manifest
def load_manifest(path: Path) -> dict:
    return json.loads(path.read_text()) if path.exists() else {}


def save_manifest(path: Path, manifest: dict) -> None:
    path.write_text(json.dumps(manifest, indent=2, default=list))


# ----------------------------------------------------------------- main
def process_month(year: int, month: int, out: Path, manifest: dict,
                  bucket: str | None, prefix: str) -> str:
    """Download, inspect, validate and upload one month.
    Returns 'skipped', 'missing', 'passed' or 'failed'."""
    key = f"{year}-{month:02d}"
    dest = out / f"bts_ontime_{year}_{month:02d}.zip"
    entry = manifest.get(key)
    if entry and entry["status"] == "passed" and dest.exists() \
            and sha256(dest) == entry["sha256"]:
        log.info("%s already downloaded and validated, skipping", key)
        return "skipped"

    if not dest.exists():
        log.info("%s downloading", key)
        if not download(URL.format(y=year, m=month), dest):
            log.info("%s not published yet", key)
            return "missing"

    stats = inspect_zip(dest)
    failures = validate_month(stats, year, month)
    stats.pop("columns")  # keep the manifest small
    stats.update(status="failed" if failures else "passed", failures=failures)
    if not failures and bucket:
        stats["s3_uri"] = upload_s3(dest, bucket, prefix, year, month)
    manifest[key] = stats
    log.info("%s %s: %d rows%s", key, stats["status"], stats["row_count"],
             "" if not failures else f" -> {failures}")
    return stats["status"]


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--months", type=int, default=12,
                   help="number of most recent published months to get")
    p.add_argument("--start", help="first month, YYYY-MM (use with --end)")
    p.add_argument("--end", help="last month, YYYY-MM")
    p.add_argument("--out", default="data/raw", help="local download folder")
    p.add_argument("--s3-bucket", help="upload passed months to this bucket")
    p.add_argument("--s3-prefix", default="raw")
    a = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    manifest_path = out / "manifest.json"
    manifest = load_manifest(manifest_path)

    results: dict[str, str] = {}
    if a.start and a.end:
        for y, m in month_range(a.start, a.end):
            results[f"{y}-{m:02d}"] = process_month(y, m, out, manifest, a.s3_bucket, a.s3_prefix)
            save_manifest(manifest_path, manifest)
    else:
        found = 0
        for y, m in months_back_from_today(a.months + LOOKBACK_MONTHS):
            status = process_month(y, m, out, manifest, a.s3_bucket, a.s3_prefix)
            save_manifest(manifest_path, manifest)
            if status != "missing":
                results[f"{y}-{m:02d}"] = status
                found += 1
            if found == a.months:
                break

    for key, msgs in volume_warnings(manifest).items():
        manifest[key]["warnings"] = msgs
        log.warning("%s volume warning: %s", key, msgs)
    save_manifest(manifest_path, manifest)

    failed = [k for k, v in results.items() if v == "failed"]
    log.info("done: %s", results)
    if failed:
        raise SystemExit(f"validation failed for {failed}; see {manifest_path}")


if __name__ == "__main__":
    main()
