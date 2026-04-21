"""
Part A — Fetch HPD violations + PLUTO coords, with checkpoint resume.
Re-runnable: already-saved pages are skipped.
"""
import time
import requests
import pandas as pd
from pathlib import Path

DATA_DIR = Path("data")
PAGES_DIR = DATA_DIR / "pages"
PAGES_DIR.mkdir(parents=True, exist_ok=True)

HPD_URL = "https://data.cityofnewyork.us/resource/wvxf-dwi5.json"
PLUTO_URL = "https://data.cityofnewyork.us/resource/64uk-42ks.json"

SINCE = "2022-01-01"
BATCH = 50000
TIMEOUT = 90
MAX_RETRIES = 5


def get_with_retry(url, params):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, timeout=TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            wait = 2 ** attempt
            print(f" [retry {attempt}/{MAX_RETRIES} in {wait}s: {type(e).__name__}]", end=" ", flush=True)
            time.sleep(wait)
    raise RuntimeError(f"Failed after {MAX_RETRIES} retries")


# ── HPD Violations (paginated, checkpointed) ────────────────────────────────

def fetch_all_violations():
    print("Fetching HPD violations (checkpointed)...")
    offset, page = 0, 1

    while True:
        out_path = PAGES_DIR / f"hpd_page_{page:04d}.parquet"
        if out_path.exists():
            existing = pd.read_parquet(out_path)
            n = len(existing)
            print(f"  Page {page} — already saved ({n:,} rows), skipping")
            if n < BATCH:
                break
            offset += BATCH
            page += 1
            continue

        print(f"  Page {page} (offset {offset:,})...", end=" ", flush=True)
        params = {
            "$limit": BATCH,
            "$offset": offset,
            "$where": f"inspectiondate > '{SINCE}'",
            "$select": "buildingid,class,violationstatus,currentstatus,currentstatusdate,inspectiondate,novdescription,housenumber,streetname,boroid,block,lot",
        }
        rows = get_with_retry(HPD_URL, params)
        n = len(rows)
        print(f"{n:,} rows", flush=True)

        if not rows:
            break

        df = pd.DataFrame(rows).rename(columns={"class": "violationclass"})
        df.to_parquet(out_path, index=False)

        if n < BATCH:
            break
        offset += BATCH
        page += 1
        time.sleep(0.3)  # polite

    print("  Concatenating pages...")
    dfs = [pd.read_parquet(p) for p in sorted(PAGES_DIR.glob("hpd_page_*.parquet"))]
    df = pd.concat(dfs, ignore_index=True)
    print(f"  Total violations: {len(df):,}")
    return df


# ── PLUTO (single pass, cached) ─────────────────────────────────────────────

def fetch_pluto():
    cache = DATA_DIR / "pluto_residential.parquet"
    if cache.exists():
        df = pd.read_parquet(cache)
        print(f"PLUTO: loaded from cache ({len(df):,} rows)")
        return df

    print("\nFetching PLUTO coordinates...")
    all_rows, offset, page = [], 0, 1
    while True:
        print(f"  Page {page} (offset {offset:,})...", end=" ", flush=True)
        params = {
            "$limit": BATCH,
            "$offset": offset,
            "$select": "borocode,block,lot,latitude,longitude,bldgclass,unitsres,landuse",
            "$where": "latitude IS NOT NULL AND landuse IN ('02','03')",
        }
        rows = get_with_retry(PLUTO_URL, params)
        n = len(rows)
        print(f"{n:,}")
        if not rows:
            break
        all_rows.extend(rows)
        if n < BATCH:
            break
        offset += BATCH
        page += 1
        time.sleep(0.3)

    df = pd.DataFrame(all_rows)
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df = df.dropna(subset=["latitude", "longitude"])
    df["bbl_key"] = (
        df["borocode"].astype(str).str.strip()
        + df["block"].astype(str).str.strip().str.zfill(5)
        + df["lot"].astype(str).str.strip().str.zfill(4)
    )
    df.to_parquet(cache, index=False)
    print(f"  PLUTO saved: {len(df):,} residential lots")
    return df


# ── Join & Save ──────────────────────────────────────────────────────────────

def build_bbl_key(df):
    df = df.copy()
    df["bbl_key"] = (
        df["boroid"].astype(str).str.strip()
        + df["block"].astype(str).str.strip().str.zfill(5)
        + df["lot"].astype(str).str.strip().str.zfill(4)
    )
    return df


def main():
    violations = fetch_all_violations()
    pluto = fetch_pluto()

    print("\nJoining on BBL...")
    violations = build_bbl_key(violations)
    joined = violations.merge(
        pluto[["bbl_key", "latitude", "longitude", "bldgclass", "unitsres"]],
        on="bbl_key",
        how="inner",
    )
    match_rate = len(joined) / len(violations) * 100
    print(f"  Matched: {len(joined):,} / {len(violations):,} ({match_rate:.1f}%)")

    print("\n=== Summary ===")
    print("violationstatus:", joined["violationstatus"].value_counts().to_dict())
    print("violationclass:", joined["violationclass"].value_counts().to_dict())
    print("Borough (boroid):", joined["boroid"].value_counts().sort_index().to_dict())

    out = DATA_DIR / "hpd_violations_raw.parquet"
    joined.to_parquet(out, index=False)
    print(f"\nSaved → {out}  ({out.stat().st_size / 1_000_000:.1f} MB)")


if __name__ == "__main__":
    main()
