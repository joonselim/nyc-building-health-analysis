"""
Part A — Step 1 (CSV version): Load HPD CSVs and produce hpd_violations_raw.parquet
Run this instead of part_a_fetch.py when you have the CSVs already downloaded.

Coordinates: replaced with PLUTO lot centroids (more accurate than HPD geocoding).
PLUTO data is fetched from NYC Open Data API and cached at data/pluto_coords.parquet.

Outputs:
  data/hpd_violations_raw.parquet  ← same schema part_a_map.py expects
"""
import time
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime

DATA_DIR = Path("data")
VIOLATIONS_CSV    = DATA_DIR / "hpd_violations.csv"
REGISTRATIONS_CSV = DATA_DIR / "hpd_registrations.csv"
PLUTO_CACHE       = DATA_DIR / "pluto_coords.parquet"
OUT               = DATA_DIR / "hpd_violations_raw.parquet"

SINCE       = "2022-01-01"
PLUTO_URL   = "https://data.cityofnewyork.us/resource/64uk-42ks.json"
BATCH       = 50000
MAX_RETRIES = 5


# ── helpers ──────────────────────────────────────────────────────────────────

def get_with_retry(url, params):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, timeout=90)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            wait = 2 ** attempt
            print(f"  [retry {attempt}/{MAX_RETRIES} in {wait}s: {type(e).__name__}]", end=" ", flush=True)
            time.sleep(wait)
    raise RuntimeError(f"Failed after {MAX_RETRIES} retries")


def bbl_key_from_parts(boro, block, lot):
    return (
        boro.astype(str).str.strip()
        + block.astype(str).str.strip().str.zfill(5)
        + lot.astype(str).str.strip().str.zfill(4)
    )


# ── 1. Violations ─────────────────────────────────────────────────────────────

def load_violations():
    print("Loading violations CSV...")
    df = pd.read_csv(
        VIOLATIONS_CSV,
        usecols=[
            "violationid", "buildingid", "registrationid",
            "boroid", "housenumber", "streetname",
            "block", "lot", "bbl",
            "class", "inspectiondate",
            "novdescription", "currentstatus", "currentstatusdate",
            "violationstatus", "latitude", "longitude",
        ],
        dtype=str,
        low_memory=False,
    )
    df = df.rename(columns={"class": "violationclass"})
    df["inspectiondate"]    = pd.to_datetime(df["inspectiondate"], errors="coerce")
    df["currentstatusdate"] = pd.to_datetime(df["currentstatusdate"], errors="coerce")
    df["closedate"]         = df["currentstatusdate"]

    # Keep HPD coords as fallback; clean them now
    df["hpd_lat"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["hpd_lon"] = pd.to_numeric(df["longitude"], errors="coerce")
    df = df.drop(columns=["latitude", "longitude"])

    before = len(df)
    df = df[df["inspectiondate"] >= SINCE]
    df = df.dropna(subset=["hpd_lat", "hpd_lon"])
    print(f"  {before:,} rows → {len(df):,} after date filter + lat/lon drop")
    return df


# ── 2. Active registrations ───────────────────────────────────────────────────

def load_active_registration_ids():
    print("Loading registrations CSV...")
    reg = pd.read_csv(
        REGISTRATIONS_CSV,
        usecols=["RegistrationID", "RegistrationEndDate"],
        dtype=str,
        low_memory=False,
    )
    reg["RegistrationEndDate"] = pd.to_datetime(reg["RegistrationEndDate"], errors="coerce")
    today = pd.Timestamp(datetime.now().date())
    active = reg[reg["RegistrationEndDate"] >= today]["RegistrationID"]
    print(f"  Active registrations: {len(active):,} / {len(reg):,}")
    return set(active.dropna().astype(str))


# ── 3. PLUTO coordinates (cached) ─────────────────────────────────────────────

def fetch_pluto():
    if PLUTO_CACHE.exists():
        df = pd.read_parquet(PLUTO_CACHE)
        print(f"PLUTO: loaded from cache ({len(df):,} lots)")
        return df

    print("Fetching PLUTO coordinates from NYC Open Data API...")
    all_rows, offset, page = [], 0, 1
    while True:
        print(f"  Page {page} (offset {offset:,})...", end=" ", flush=True)
        params = {
            "$limit":  BATCH,
            "$offset": offset,
            "$select": "borocode,block,lot,latitude,longitude",
            "$where":  "latitude IS NOT NULL",
        }
        rows = get_with_retry(PLUTO_URL, params)
        n = len(rows)
        print(f"{n:,} rows")
        if not rows:
            break
        all_rows.extend(rows)
        if n < BATCH:
            break
        offset += BATCH
        page   += 1
        time.sleep(0.3)

    df = pd.DataFrame(all_rows)
    df["latitude"]  = pd.to_numeric(df["latitude"],  errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df = df.dropna(subset=["latitude", "longitude"])
    df["bbl_key"] = bbl_key_from_parts(df["borocode"], df["block"], df["lot"])
    df = df[["bbl_key", "latitude", "longitude"]].drop_duplicates("bbl_key")

    df.to_parquet(PLUTO_CACHE, index=False)
    print(f"  PLUTO cached: {len(df):,} lots → {PLUTO_CACHE}")
    return df


# ── 4. Join & emit ────────────────────────────────────────────────────────────

def main():
    viol = load_violations()
    active_ids = load_active_registration_ids()

    before = len(viol)
    viol = viol[viol["registrationid"].isin(active_ids)]
    print(f"  After active-registration filter: {len(viol):,} / {before:,} violations")

    pluto = fetch_pluto()

    # Build BBL key from violations (bbl column is already the 10-digit BBL string)
    # Format: 1 digit boro + 5 digit block + 4 digit lot, no spaces
    viol["bbl_clean"] = viol["bbl"].astype(str).str.strip().str.replace(r"\D", "", regex=True)
    # Also build from parts as fallback
    viol["bbl_from_parts"] = bbl_key_from_parts(viol["boroid"], viol["block"], viol["lot"])

    # Try bbl column first, fall back to parts
    viol["bbl_key"] = viol["bbl_clean"].where(viol["bbl_clean"].str.len() == 10, viol["bbl_from_parts"])

    print("\nJoining with PLUTO coordinates...")
    viol = viol.merge(pluto.rename(columns={"latitude": "pluto_lat", "longitude": "pluto_lon"}),
                      on="bbl_key", how="left")

    matched = viol["pluto_lat"].notna().sum()
    print(f"  PLUTO match: {matched:,} / {len(viol):,} ({matched/len(viol)*100:.1f}%)")

    # Use PLUTO coords where available, fall back to HPD coords
    viol["latitude"]  = viol["pluto_lat"].fillna(viol["hpd_lat"])
    viol["longitude"] = viol["pluto_lon"].fillna(viol["hpd_lon"])
    viol = viol.drop(columns=["hpd_lat", "hpd_lon", "pluto_lat", "pluto_lon",
                               "bbl_clean", "bbl_from_parts", "bbl_key"])

    viol.to_parquet(OUT, index=False)
    size_mb = OUT.stat().st_size / 1_000_000
    print(f"\nSaved → {OUT}  ({size_mb:.1f} MB)")
    print("\nviolationclass distribution:")
    print(viol["violationclass"].value_counts().to_dict())


if __name__ == "__main__":
    main()
