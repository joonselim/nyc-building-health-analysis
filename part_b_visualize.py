"""
Part B — Building Health Score: visualizations + summary table
Run after part_a_map.py (reads data/buildings_scored.parquet)

Outputs:
  score_distribution.png
  borough_breakdown.png
  score_components.png
  top20_worst.csv
"""
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from pathlib import Path

DATA_DIR = Path("data")
INPUT = DATA_DIR / "buildings_scored.parquet"

BORO_MAP = {"1": "Manhattan", "2": "Bronx", "3": "Brooklyn", "4": "Queens", "5": "Staten Island"}

COLORS = {
    "Poorly managed": "#d73027",
    "Struggling":     "#fc8d59",
    "Moderate":       "#fee090",
    "Well-managed":   "#4575b4",
}
LABEL_ORDER = ["Poorly managed", "Struggling", "Moderate", "Well-managed"]

plt.rcParams.update({
    "font.family": "sans-serif",
    "axes.spines.top": False,
    "axes.spines.right": False,
})


def load():
    df = pd.read_parquet(INPUT)
    df["borough"] = df["boroid"].map(BORO_MAP).fillna("Unknown")
    df["label"] = pd.Categorical(df["label"], categories=LABEL_ORDER, ordered=True)
    return df


# ── 1. Score distribution histogram ─────────────────────────────────────────

def plot_distribution(df):
    fig, ax = plt.subplots(figsize=(11, 5))

    thresholds = sorted(df["health_score"].quantile([0.10, 0.25, 0.50]).values)
    colors_per_bin = [COLORS["Poorly managed"], COLORS["Struggling"],
                      COLORS["Moderate"], COLORS["Well-managed"]]

    bins = [0] + list(thresholds) + [100]
    for i in range(len(bins) - 1):
        mask = (df["health_score"] >= bins[i]) & (df["health_score"] <= bins[i + 1])
        ax.hist(df.loc[mask, "health_score"], bins=30,
                color=colors_per_bin[i], edgecolor="white", linewidth=0.4, alpha=0.9)

    y_top = ax.get_ylim()[1]
    y_levels = [0.95, 0.80, 0.65]
    for i, (t, lbl) in enumerate(zip(thresholds, ["Hot prospects", "Warm prospects", "Median"])):
        ax.axvline(t, color="#444", linestyle="--", linewidth=0.9, alpha=0.7)
        ax.text(t + 0.5, y_top * y_levels[i], lbl,
                fontsize=8, color="#444", va="top")

    patches = [mpatches.Patch(color=COLORS[l], label=l) for l in LABEL_ORDER]
    ax.legend(handles=patches, loc="upper left", fontsize=9, framealpha=0.8)

    ax.set_xlabel("Building Health Score  (higher = better managed)", fontsize=11)
    ax.set_ylabel("Number of Buildings", fontsize=11)
    ax.set_title("NYC Building Health Score Distribution\n"
                 f"({len(df):,} buildings with open HPD violations since 2022)",
                 fontsize=13, fontweight="bold", pad=12)

    plt.tight_layout()
    plt.savefig("score_distribution.png", dpi=150)
    plt.close()
    print("Saved → score_distribution.png")


# ── 2. Borough breakdown ─────────────────────────────────────────────────────

def plot_borough(df):
    fig, axes = plt.subplots(1, 2, figsize=(13, 5))

    # Left: avg health score per borough
    boro_avg = (
        df.groupby("borough")["health_score"]
        .agg(["mean", "count"])
        .rename(columns={"mean": "avg_score", "count": "n_buildings"})
        .reindex(["Manhattan", "Bronx", "Brooklyn", "Queens", "Staten Island"])
        .dropna()
    )
    bars = axes[0].bar(
        boro_avg.index, boro_avg["avg_score"],
        color="#4575b4", edgecolor="white", linewidth=0.5
    )
    for bar, (_, row) in zip(bars, boro_avg.iterrows()):
        axes[0].text(bar.get_x() + bar.get_width() / 2,
                     bar.get_height() + 0.3,
                     f"{row['avg_score']:.1f}",
                     ha="center", va="bottom", fontsize=9, fontweight="bold")
    axes[0].set_ylabel("Avg Health Score (higher = better)", fontsize=10)
    axes[0].set_title("Average Building Health Score by Borough", fontsize=11, fontweight="bold")
    axes[0].set_ylim(0, 100)
    axes[0].tick_params(axis="x", labelsize=9)

    # Right: stacked bar — label breakdown per borough
    pivot = (
        df.groupby(["borough", "label"], observed=True)
        .size()
        .unstack(fill_value=0)
        .reindex(["Manhattan", "Bronx", "Brooklyn", "Queens", "Staten Island"])
        .dropna()
    )
    pivot_pct = pivot.div(pivot.sum(axis=1), axis=0) * 100

    bottom = pd.Series(0.0, index=pivot_pct.index)
    for label in LABEL_ORDER:
        if label not in pivot_pct.columns:
            continue
        axes[1].bar(pivot_pct.index, pivot_pct[label],
                    bottom=bottom, color=COLORS[label],
                    edgecolor="white", linewidth=0.4, label=label)
        bottom += pivot_pct[label]

    axes[1].set_ylabel("% of Buildings", fontsize=10)
    axes[1].set_title("Label Distribution by Borough", fontsize=11, fontweight="bold")
    axes[1].set_ylim(0, 100)
    axes[1].tick_params(axis="x", labelsize=9)
    handles = [mpatches.Patch(color=COLORS[l], label=l) for l in LABEL_ORDER]
    axes[1].legend(handles=handles, fontsize=8, loc="lower right", framealpha=0.8)

    plt.tight_layout()
    plt.savefig("borough_breakdown.png", dpi=150)
    plt.close()
    print("Saved → borough_breakdown.png")


# ── 3. Score component radar / bar ───────────────────────────────────────────

def plot_components(df):
    components = {
        "Open\nViolations (30%)":  "score_open",
        "Severity (25%)":          "score_severity",
        "Resolution\nSpeed (20%)": "score_resolution",
        "Recency (15%)":           "score_recency",
        "Repeat\nRate (10%)":      "score_repeat",
    }

    label_means = {
        label: df[df["label"] == label][list(components.values())].mean()
        for label in LABEL_ORDER
    }

    x = range(len(components))
    fig, ax = plt.subplots(figsize=(11, 5))
    width = 0.2

    for i, label in enumerate(LABEL_ORDER):
        vals = [label_means[label][col] for col in components.values()]
        offset = (i - 1.5) * width
        ax.bar([xi + offset for xi in x], vals,
               width=width, color=COLORS[label], label=label,
               edgecolor="white", linewidth=0.4)

    ax.set_xticks(list(x))
    ax.set_xticklabels(list(components.keys()), fontsize=10)
    ax.set_ylabel("Component Score (0–100)", fontsize=10)
    ax.set_ylim(0, 105)
    ax.set_title("Score Component Breakdown by Building Label\n"
                 "(higher = better managed on that dimension)",
                 fontsize=12, fontweight="bold", pad=10)
    ax.legend(fontsize=9, framealpha=0.8)

    plt.tight_layout()
    plt.savefig("score_components.png", dpi=150)
    plt.close()
    print("Saved → score_components.png")


# ── 4. Top 20 worst table ────────────────────────────────────────────────────

def export_top20(df):
    worst = df.nsmallest(20, "health_score")[[
        "housenumber", "streetname", "borough", "health_score", "label",
        "total_open", "class_c", "class_b", "avg_resolution_days", "recent_count"
    ]].copy()
    worst["avg_resolution_days"] = worst["avg_resolution_days"].round(0).astype(int)
    worst["recent_count"] = worst["recent_count"].astype(int)
    worst.columns = [
        "House #", "Street", "Borough", "Health Score", "Label",
        "Open Violations", "Class C", "Class B", "Avg Resolution (days)", "Recent (6mo)"
    ]
    worst.to_csv("top20_worst.csv", index=False)
    print("Saved → top20_worst.csv\n")
    print("=== Top 20 Worst-Managed Buildings ===")
    print(worst.to_string(index=False))


def main():
    print("Loading scored buildings...")
    df = load()
    print(f"  {len(df):,} buildings\n")

    print("Label distribution:")
    print(df["label"].value_counts().sort_index())
    print()

    plot_distribution(df)
    plot_borough(df)
    plot_components(df)
    print()
    export_top20(df)


if __name__ == "__main__":
    main()
