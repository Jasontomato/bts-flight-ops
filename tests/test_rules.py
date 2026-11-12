"""
Checks your validate_month() and volume_warnings() implementations.
 
Run from the project folder, pointing at the December 2025 zip:
    python tests/test_rules.py data/raw/bts_ontime_2025_12.zip
Each line prints PASS or FAIL. All PASS means your rules behave as designed.
"""
import copy
import sys
from pathlib import Path
 
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingestion"))
import download_bts as d  # noqa: E402
 
 
def check(name, condition):
    print(("PASS  " if condition else "FAIL  ") + name)
    return condition
 
 
def main(zip_path):
    good = d.inspect_zip(Path(zip_path))   # a known-good month: 2025-12
    results = []
 
    # A correct file passes every rule
    results.append(check("good December file has no failures",
                         d.validate_month(copy.deepcopy(good), 2025, 12) == []))
 
    # Rule 1: a renamed column must fail
    bad = copy.deepcopy(good)
    bad["columns"][5] = "RenamedColumn"
    results.append(check("rule 1 catches a renamed column",
                         len(d.validate_month(bad, 2025, 12)) > 0))
 
    # Rule 1: a missing column must fail
    bad = copy.deepcopy(good)
    bad["columns"] = bad["columns"][:-2] + bad["columns"][-1:]
    results.append(check("rule 1 catches a missing column",
                         len(d.validate_month(bad, 2025, 12)) > 0))
 
    # Rule 3: requesting November but receiving December must fail
    results.append(check("rule 3 catches the wrong month",
                         len(d.validate_month(copy.deepcopy(good), 2025, 11)) > 0))
 
    # Rule 3: two months in one file must fail
    bad = copy.deepcopy(good)
    bad["year_months_in_file"] = [(2025, 11), (2025, 12)]
    results.append(check("rule 3 catches a file with two months",
                         len(d.validate_month(bad, 2025, 12)) > 0))
 
    # Rule 5: the last five days missing must fail
    bad = copy.deepcopy(good)
    bad["flight_dates"] = bad["flight_dates"][:-5]
    results.append(check("rule 5 catches five missing days",
                         len(d.validate_month(bad, 2025, 12)) > 0))
 
    # Rule 2: one month with roughly half the normal volume gets a warning
    manifest = {
        "2025-09": {"status": "passed", "row_count": 570000},   # 30 days
        "2025-10": {"status": "passed", "row_count": 590000},   # 31 days
        "2025-11": {"status": "passed", "row_count": 285000},   # 30 days, about half
        "2025-12": {"status": "passed", "row_count": 582304},   # 31 days
        "2026-01": {"status": "passed", "row_count": 575000},   # 31 days
    }
    warnings = d.volume_warnings(manifest)
    results.append(check("rule 2 warns on the half-volume month",
                         "2025-11" in warnings))
    results.append(check("rule 2 does not warn on normal months",
                         set(warnings) == {"2025-11"}))
 
    # Rule 2: failed months are ignored, and too little history means no warnings
    small = {"2025-11": {"status": "passed", "row_count": 285000},
             "2025-12": {"status": "passed", "row_count": 582304},
             "2026-01": {"status": "failed", "row_count": 100}}
    results.append(check("rule 2 skips when there is too little history",
                         d.volume_warnings(small) == {}))
 
    # Rule 2: compares flights per DAY, so a normal February (28 days) is not flagged.
    # February's daily rate here is 15% below the others: inside the 20% band,
    # but its raw row count is about 23% lower, so a row-count-only check would warn.
    rate = 18800
    feb = {
        "2025-09": {"status": "passed", "row_count": rate * 30},
        "2025-10": {"status": "passed", "row_count": rate * 31},
        "2025-12": {"status": "passed", "row_count": rate * 31},
        "2026-01": {"status": "passed", "row_count": rate * 31},
        "2026-02": {"status": "passed", "row_count": int(rate * 0.85) * 28},
    }
    results.append(check("rule 2 adjusts for days in the month",
                         "2026-02" not in d.volume_warnings(feb)))
 
    # Rule 2: needs at least 3 OTHER months; with only 2 others it must skip
    three = {"2025-10": {"status": "passed", "row_count": 590000},
             "2025-11": {"status": "passed", "row_count": 285000},
             "2025-12": {"status": "passed", "row_count": 582304}}
    results.append(check("rule 2 needs at least 3 other months",
                         d.volume_warnings(three) == {}))
 
    print(f"\n{sum(results)}/{len(results)} checks passed")
 
 
if __name__ == "__main__":
    main(sys.argv[1])