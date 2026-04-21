"""
Export Kaggle-ready files:
  kaggle/nyc_building_health_scores.csv   ← main dataset
  kaggle/analysis.py                      ← self-contained reproducible script
"""
import pandas as pd
from pathlib import Path

DATA_DIR = Path("data")
KAGGLE_DIR = Path("kaggle")
KAGGLE_DIR.mkdir(exist_ok=True)

BORO_MAP = {"1": "Manhattan", "2": "Bronx", "3": "Brooklyn", "4": "Queens", "5": "Staten Island"}


def export_csv():
    df = pd.read_parquet(DATA_DIR / "buildings_scored.parquet")
    df["borough"] = df["boroid"].map(BORO_MAP)
    df["address"] = (df["housenumber"].fillna("") + " " + df["streetname"].fillna("")).str.strip()

    out = df[[
        "buildingid", "address", "borough",
        "latitude", "longitude",
        "health_score", "label",
        "total_open", "class_a", "class_b", "class_c",
        "weighted_severity", "avg_resolution_days",
        "recent_count", "repeat_types",
        "score_open", "score_severity", "score_resolution", "score_recency", "score_repeat",
    ]].copy()

    out = out.rename(columns={
        "total_open":          "open_violation_count",
        "class_a":             "class_a_count",
        "class_b":             "class_b_count",
        "class_c":             "class_c_count",
        "recent_count":        "violations_last_6mo",
        "repeat_types":        "repeat_violation_types",
        "score_open":          "component_open_count",
        "score_severity":      "component_severity",
        "score_resolution":    "component_resolution_speed",
        "score_recency":       "component_recency",
        "score_repeat":        "component_repeat_rate",
    })

    out["avg_resolution_days"] = out["avg_resolution_days"].round(1)
    out = out.sort_values("health_score")

    path = KAGGLE_DIR / "nyc_building_health_scores.csv"
    out.to_csv(path, index=False)
    print(f"Saved → {path}  ({len(out):,} rows, {path.stat().st_size // 1024} KB)")
    return out


if __name__ == "__main__":
    export_csv()
