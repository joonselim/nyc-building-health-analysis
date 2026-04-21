"""
NYC Building Health Score — Full Reproducible Pipeline
Author: Joonse Lim | Duke Fuqua MBA

Dataset: NYC HPD Housing Maintenance Code Violations + Multiple Dwelling Registrations
Source:  https://opendata.cityofnewyork.us/

To run on Kaggle:
  1. Add both HPD datasets as input (see README in dataset description)
  2. Set VIOLATIONS_PATH and REGISTRATIONS_PATH below
  3. Run All

Outputs:
  nyc_building_health_scores.csv
  score_distribution.png
  borough_breakdown.png
  score_components.png
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from datetime import datetime, timedelta
from pathlib import Path

# ── Paths (update if needed) ─────────────────────────────────────────────────
VIOLATIONS_PATH    = "/kaggle/input/datasets/joonselim/hpd-violations/hpd_violations.csv"
REGISTRATIONS_PATH = "/kaggle/input/datasets/joonselim/hpd-registrations/hpd_registrations.csv"
OUTPUT_DIR         = Path("/kaggle/working")

SINCE = "2022-01-01"
BORO_MAP = {"1": "Manhattan", "2": "Bronx", "3": "Brooklyn", "4": "Queens", "5": "Staten Island"}
LABEL_ORDER = ["Poorly managed", "Struggling", "Moderate", "Well-managed"]
COLORS = {
    "Poorly managed": "#d73027",
    "Struggling":     "#fc8d59",
    "Moderate":       "#fee090",
    "Well-managed":   "#4575b4",
}

plt.rcParams.update({"font.family": "sans-serif",
                     "axes.spines.top": False, "axes.spines.right": False})


# ── 1. Load & clean violations ───────────────────────────────────────────────

def load_violations(path):
    df = pd.read_csv(   
        path,
        usecols=[
            "violationid", "buildingid", "registrationid",
            "boroid", "housenumber", "streetname",
            "block", "lot",
            "class", "inspectiondate",
            "novdescription", "currentstatus", "currentstatusdate",
            "violationstatus", "latitude", "longitude",
        ],
        dtype=str, low_memory=False,
    )
    df = df.rename(columns={"class": "violationclass"})
    df["inspectiondate"]    = pd.to_datetime(df["inspectiondate"], errors="coerce")
    df["currentstatusdate"] = pd.to_datetime(df["currentstatusdate"], errors="coerce")
    df["closedate"]         = df["currentstatusdate"]   # proxy for resolution date
    df["latitude"]          = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"]         = pd.to_numeric(df["longitude"], errors="coerce")

    df = df[df["inspectiondate"] >= SINCE]
    df = df.dropna(subset=["latitude", "longitude"])
    print(f"Violations loaded: {len(df):,} rows")
    return df


def load_active_registrations(path):
    reg = pd.read_csv(path, usecols=["RegistrationID", "RegistrationEndDate"],
                      dtype=str, low_memory=False)
    reg["RegistrationEndDate"] = pd.to_datetime(reg["RegistrationEndDate"], errors="coerce")
    today = pd.Timestamp(datetime.now().date())
    active = reg[reg["RegistrationEndDate"] >= today]["RegistrationID"]
    print(f"Active registrations: {len(active):,} / {len(reg):,}")
    return set(active.dropna().astype(str))


# ── 2. Aggregate per building ─────────────────────────────────────────────────

def aggregate_buildings(df):
    open_df = df[df["violationstatus"].str.upper() == "OPEN"].copy()
    print(f"Open violations: {len(open_df):,}")

    base = open_df.groupby("buildingid").agg(
        open_violation_count=("violationclass", "count"),
        class_c_count=("violationclass", lambda x: (x == "C").sum()),
        class_b_count=("violationclass", lambda x: (x == "B").sum()),
        class_a_count=("violationclass", lambda x: (x == "A").sum()),
        latitude=("latitude", "first"),
        longitude=("longitude", "first"),
        housenumber=("housenumber", "first"),
        streetname=("streetname", "first"),
        boroid=("boroid", "first"),
    ).reset_index()

    base["weighted_severity"] = (
        base["class_c_count"] * 3 +
        base["class_b_count"] * 2 +
        base["class_a_count"] * 1
    )

    cutoff = datetime.now() - timedelta(days=180)
    recent = open_df[open_df["inspectiondate"] > cutoff]
    recent_counts = recent.groupby("buildingid").size().reset_index(name="violations_last_6mo")
    base = base.merge(recent_counts, on="buildingid", how="left").fillna({"violations_last_6mo": 0})

    repeat = open_df.groupby(["buildingid", "novdescription"]).size().reset_index(name="cnt")
    repeat = repeat[repeat["cnt"] > 1].groupby("buildingid").size().reset_index(name="repeat_violation_types")
    base = base.merge(repeat, on="buildingid", how="left").fillna({"repeat_violation_types": 0})

    closed_df = df[df["violationstatus"].str.upper() == "CLOSE"].copy()
    closed_df["days_to_resolve"] = (closed_df["closedate"] - closed_df["inspectiondate"]).dt.days
    closed_df = closed_df[closed_df["days_to_resolve"].between(0, 1825)]
    avg_res = closed_df.groupby("buildingid")["days_to_resolve"].mean().reset_index(name="avg_resolution_days")
    base = base.merge(avg_res, on="buildingid", how="left")
    base["avg_resolution_days"] = base["avg_resolution_days"].fillna(base["avg_resolution_days"].median())

    print(f"Buildings aggregated: {len(base):,}")
    return base


# ── 3. Health score ──────────────────────────────────────────────────────────

def compute_health_score(df):
    def norm_inv(s):
        mn, mx = s.min(), s.max()
        if mx == mn:
            return pd.Series(50.0, index=s.index)
        return (1 - (s - mn) / (mx - mn)) * 100

    df["component_open_count"]      = norm_inv(df["open_violation_count"])
    df["component_severity"]        = norm_inv(df["weighted_severity"])
    df["component_resolution_speed"]= norm_inv(df["avg_resolution_days"])
    df["component_recency"]         = norm_inv(df["violations_last_6mo"])
    df["component_repeat_rate"]     = norm_inv(df["repeat_violation_types"])

    df["health_score"] = (
        df["component_open_count"]       * 0.30 +
        df["component_severity"]         * 0.25 +
        df["component_resolution_speed"] * 0.20 +
        df["component_recency"]          * 0.15 +
        df["component_repeat_rate"]      * 0.10
    ).round(1)

    bins = [0, 70, 80, 90, 100]
    labels = ["Poorly managed", "Struggling", "Moderate", "Well-managed"]
    df["label"] = pd.cut(df["health_score"], bins=bins, labels=labels, include_lowest=True)

    df["borough"] = df["boroid"].map(BORO_MAP)
    df["address"] = (df["housenumber"].fillna("") + " " + df["streetname"].fillna("")).str.strip()

    print("\nLabel distribution:")
    print(df["label"].value_counts().reindex(LABEL_ORDER))
    return df


# ── 4. Visualizations ────────────────────────────────────────────────────────

def plot_distribution(df):
    fig, ax = plt.subplots(figsize=(11, 5))
    thresholds = sorted(df["health_score"].quantile([0.10, 0.25, 0.50]).values)
    color_list = [COLORS[l] for l in LABEL_ORDER]
    bins_edges = [0] + list(thresholds) + [100]
    for i in range(len(bins_edges) - 1):
        mask = (df["health_score"] >= bins_edges[i]) & (df["health_score"] <= bins_edges[i+1])
        ax.hist(df.loc[mask, "health_score"], bins=30, color=color_list[i],
                edgecolor="white", linewidth=0.4, alpha=0.9)
    y_top = ax.get_ylim()[1]
    y_levels = [0.95, 0.80, 0.65]
    for i, t in enumerate(thresholds):
        ax.axvline(t, color="#444", linestyle="--", linewidth=0.9, alpha=0.6)
        lbl = ["Hot prospects", "Warm prospects", "Median"][i]
        ax.text(t + 0.5, y_top * y_levels[i], lbl, fontsize=8, color="#444", va="top")
    patches = [mpatches.Patch(color=COLORS[l], label=l) for l in LABEL_ORDER]
    ax.legend(handles=patches, fontsize=9)
    ax.set_xlabel("Building Health Score  (higher = better managed)", fontsize=11)
    ax.set_ylabel("Number of Buildings", fontsize=11)
    ax.set_title(f"NYC Building Health Score Distribution ({len(df):,} buildings)",
                 fontsize=13, fontweight="bold")
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "score_distribution.png", dpi=150)
    plt.show()
    plt.close()
    print("Saved: score_distribution.png")


def plot_borough(df):
    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    boro_order = ["Manhattan", "Bronx", "Brooklyn", "Queens", "Staten Island"]

    boro_avg = df.groupby("borough")["health_score"].mean().reindex(boro_order).dropna()
    bars = axes[0].bar(boro_avg.index, boro_avg.values, color="#4575b4",
                       edgecolor="white", linewidth=0.5)
    for bar, val in zip(bars, boro_avg.values):
        axes[0].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.3,
                     f"{val:.1f}", ha="center", va="bottom", fontsize=9, fontweight="bold")
    axes[0].set_ylim(0, 100)
    axes[0].set_title("Average Health Score by Borough", fontsize=11, fontweight="bold")
    axes[0].set_ylabel("Avg Health Score (higher = better)")

    pivot = (df.groupby(["borough", "label"], observed=True)
               .size().unstack(fill_value=0)
               .reindex(boro_order).dropna())
    pivot_pct = pivot.div(pivot.sum(axis=1), axis=0) * 100
    bottom = pd.Series(0.0, index=pivot_pct.index)
    for label in LABEL_ORDER:
        if label not in pivot_pct.columns:
            continue
        axes[1].bar(pivot_pct.index, pivot_pct[label], bottom=bottom,
                    color=COLORS[label], edgecolor="white", linewidth=0.4, label=label)
        bottom += pivot_pct[label]
    axes[1].set_ylim(0, 100)
    axes[1].set_title("Label Distribution by Borough", fontsize=11, fontweight="bold")
    axes[1].set_ylabel("% of Buildings")
    handles = [mpatches.Patch(color=COLORS[l], label=l) for l in LABEL_ORDER]
    axes[1].legend(handles=handles, fontsize=8, loc="lower right")

    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "borough_breakdown.png", dpi=150)
    plt.show()
    plt.close()
    print("Saved: borough_breakdown.png")


def plot_components(df):
    components = {
        "Open\nViolations\n(30%)":  "component_open_count",
        "Severity\n(25%)":          "component_severity",
        "Resolution\nSpeed\n(20%)": "component_resolution_speed",
        "Recency\n(15%)":           "component_recency",
        "Repeat\nRate\n(10%)":      "component_repeat_rate",
    }
    label_means = {
        l: df[df["label"] == l][list(components.values())].mean()
        for l in LABEL_ORDER
    }
    x = range(len(components))
    width = 0.2
    fig, ax = plt.subplots(figsize=(11, 5))
    for i, label in enumerate(LABEL_ORDER):
        vals = [label_means[label][col] for col in components.values()]
        offsets = [xi + (i - 1.5) * width for xi in x]
        ax.bar(offsets, vals, width=width, color=COLORS[label],
               label=label, edgecolor="white", linewidth=0.4)
    ax.set_xticks(list(x))
    ax.set_xticklabels(list(components.keys()), fontsize=9)
    ax.set_ylabel("Component Score (0–100, higher = better)")
    ax.set_ylim(0, 105)
    ax.set_title("Score Components by Building Label", fontsize=12, fontweight="bold")
    ax.legend(fontsize=9)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "score_components.png", dpi=150)
    plt.show()
    plt.close()
    print("Saved: score_components.png")


# ── 5. Export CSV ────────────────────────────────────────────────────────────

def export_csv(df):
    out = df[[
        "buildingid", "address", "borough", "latitude", "longitude",
        "health_score", "label",
        "open_violation_count", "class_a_count", "class_b_count", "class_c_count",
        "weighted_severity", "avg_resolution_days",
        "violations_last_6mo", "repeat_violation_types",
        "component_open_count", "component_severity",
        "component_resolution_speed", "component_recency", "component_repeat_rate",
    ]].sort_values("health_score").copy()
    out["avg_resolution_days"] = out["avg_resolution_days"].round(1)

    path = OUTPUT_DIR / "nyc_building_health_scores.csv"
    out.to_csv(path, index=False)
    print(f"\nSaved: nyc_building_health_scores.csv  ({len(out):,} rows)")

    print("\n=== Top 10 Worst-Managed Buildings ===")
    print(out.head(10)[["address", "borough", "health_score", "label",
                         "open_violation_count", "class_c_count",
                         "avg_resolution_days"]].to_string(index=False))


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=== NYC Building Health Score Pipeline ===\n")

    viol = load_violations(VIOLATIONS_PATH)
    active_ids = load_active_registrations(REGISTRATIONS_PATH)

    before = len(viol)
    viol = viol[viol["registrationid"].isin(active_ids)]
    print(f"After active-registration filter: {len(viol):,} / {before:,}\n")

    bldg = aggregate_buildings(viol)
    bldg = compute_health_score(bldg)

    plot_distribution(bldg)
    plot_borough(bldg)
    plot_components(bldg)
    export_csv(bldg)

    print("\nDone.")


if __name__ == "__main__":
    main()
