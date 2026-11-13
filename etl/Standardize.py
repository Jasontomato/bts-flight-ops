"""
Standardize validated raw BTS files into typed Parquet for loading into Redshift.

This is the "light" step of the ELT design: it keeps the useful columns, sets
their data types, and records where each row came from. It deliberately does
NOT apply the data quality rules: DQ-1 to DQ-8 run later in SQL, in Redshift.

For each month that passed download validation (data/raw/manifest.json):
    raw zip -> read 49 of the 110 columns with explicit types
            -> <out>/flights/year=YYYY/month=MM/flights.parquet
            -> optionally s3://<bucket>/staging/flights/year=YYYY/month=MM/

Examples
    python etl/standardize.py --only 2025-12
    python etl/standardize.py --s3-bucket my-bucket

Re-running a month overwrites its file, so the job is safe to repeat.
"""
from __future__ import annotations

import argparse
import json
import logging
import zipfile
from pathlib import Path

import pandas as pd

log = logging.getLogger("standardize")

# Columns we keep, with their types. The other 61 columns are dropped,
# including the 33 that are always empty.
# Clock times stay as text so leading zeros survive ("0005", not 5);
# converting them is part of DQ-1 in SQL.
COLUMNS = {
    # identity
    "FlightDate": "string", "Year": "Int32", "Month": "Int32",
    "DayofMonth": "Int32", "DayOfWeek": "Int32",
    "Reporting_Airline": "string", "DOT_ID_Reporting_Airline": "Int32",
    "IATA_CODE_Reporting_Airline": "string",
    "Flight_Number_Reporting_Airline": "Int32", "Tail_Number": "string",
    # airports
    "Origin": "string", "OriginAirportID": "Int32", "OriginAirportSeqID": "Int32",
    "OriginCityMarketID": "Int32", "OriginCityName": "string", "OriginState": "string",
    "Dest": "string", "DestAirportID": "Int32", "DestAirportSeqID": "Int32",
    "DestCityMarketID": "Int32", "DestCityName": "string", "DestState": "string",
    # clock times (hhmm, local time)
    "CRSDepTime": "string", "DepTime": "string", "WheelsOff": "string",
    "WheelsOn": "string", "CRSArrTime": "string", "ArrTime": "string",
    # delays and durations, in minutes
    "DepDelay": "float64", "DepDelayMinutes": "float64", "DepDel15": "float64",
    "TaxiOut": "float64", "TaxiIn": "float64",
    "ArrDelay": "float64", "ArrDelayMinutes": "float64", "ArrDel15": "float64",
    "CRSElapsedTime": "float64", "ActualElapsedTime": "float64",
    "AirTime": "float64", "Distance": "float64",
    # status
    "Cancelled": "float64", "CancellationCode": "string",
    "Diverted": "float64", "DivReachedDest": "float64",
    # delay causes, in minutes
    "CarrierDelay": "float64", "WeatherDelay": "float64", "NASDelay": "float64",
    "SecurityDelay": "float64", "LateAircraftDelay": "float64",
}


def read_month(zip_path: Path) -> pd.DataFrame:
    """Read the kept columns from the CSV inside a raw zip, with explicit types."""
    with zipfile.ZipFile(zip_path) as z:
        csv_name = [n for n in z.namelist() if n.lower().endswith(".csv")][0]
        with z.open(csv_name) as f:
            return pd.read_csv(f, usecols=list(COLUMNS), dtype=COLUMNS)


def standardize(df: pd.DataFrame, source_file: str) -> pd.DataFrame:
    """Light, rule-free clean-up before loading."""
    df["FlightDate"] = pd.to_datetime(df["FlightDate"], format="%Y-%m-%d").dt.date
    df["source_file"] = source_file          # lineage: which raw file each row came from
    return df


def process(key: str, raw_dir: Path, out: Path, manifest: dict, bucket: str | None) -> None:
    year, month = (int(x) for x in key.split("-"))
    zip_path = raw_dir / f"bts_ontime_{year}_{month:02d}.zip"
    df = standardize(read_month(zip_path), zip_path.name)

    # Sanity check: no rows lost between the raw file and the output
    expected = manifest[key]["row_count"]
    if len(df) != expected:
        raise ValueError(f"{key}: read {len(df)} rows, manifest says {expected}")

    part = f"year={year}/month={month:02d}"
    path = out / "flights" / part / "flights.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    msg = f"{key}: {len(df):,} rows -> {path} ({path.stat().st_size / 1e6:.1f} MB)"

    if bucket:
        import boto3
        s3_key = f"staging/flights/{part}/flights.parquet"
        boto3.client("s3").upload_file(str(path), bucket, s3_key)
        msg += f", uploaded to s3://{bucket}/{s3_key}"
    log.info(msg)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--raw-dir", default="data/raw")
    p.add_argument("--out", default="data/staging")
    p.add_argument("--only", help="process a single month, YYYY-MM")
    p.add_argument("--s3-bucket", help="also upload to this bucket")
    a = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    raw_dir, out = Path(a.raw_dir), Path(a.out)
    manifest = json.loads((raw_dir / "manifest.json").read_text())
    months = [a.only] if a.only else sorted(k for k, v in manifest.items() if v["status"] == "passed")
    for key in months:
        process(key, raw_dir, out, manifest, a.s3_bucket)


if __name__ == "__main__":
    main()