"""
Part A — Step 2: Aggregate + score + build interactive map
Run AFTER part_a_fetch.py
"""
import pandas as pd
import numpy as np
import folium
from folium.plugins import MarkerCluster
from pathlib import Path
from datetime import datetime, timedelta

DATA_DIR = Path("data")
INPUT = DATA_DIR / "hpd_violations_raw.parquet"

def load_and_clean(path):
    df = pd.read_parquet(path)
    df["inspectiondate"] = pd.to_datetime(df["inspectiondate"], errors="coerce")
    df["closedate"] = pd.to_datetime(df["closedate"], errors="coerce")
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df = df.dropna(subset=["latitude", "longitude"])
    return df

def aggregate_buildings(df):
    # Open violations (violationstatus = "Open" is the canonical flag in NYC HPD data)
    open_df = df[df["violationstatus"].str.upper() == "OPEN"].copy()

    base = open_df.groupby("buildingid").agg(
        total_open=("violationclass", "count"),
        class_c=("violationclass", lambda x: (x == "C").sum()),
        class_b=("violationclass", lambda x: (x == "B").sum()),
        class_a=("violationclass", lambda x: (x == "A").sum()),
        latitude=("latitude", "first"),
        longitude=("longitude", "first"),
        housenumber=("housenumber", "first"),
        streetname=("streetname", "first"),
        boroid=("boroid", "first"),
    ).reset_index()

    # Weighted severity
    base["weighted_severity"] = base["class_c"] * 3 + base["class_b"] * 2 + base["class_a"] * 1

    # Recent violations (last 6 months)
    cutoff = datetime.now() - timedelta(days=180)
    recent = open_df[open_df["inspectiondate"] > cutoff]
    recent_counts = recent.groupby("buildingid").size().reset_index(name="recent_count")
    base = base.merge(recent_counts, on="buildingid", how="left")
    base["recent_count"] = base["recent_count"].fillna(0)

    # Repeat violation types
    repeat = open_df.groupby(["buildingid", "novdescription"]).size().reset_index(name="cnt")
    repeat = repeat[repeat["cnt"] > 1].groupby("buildingid").size().reset_index(name="repeat_types")
    base = base.merge(repeat, on="buildingid", how="left")
    base["repeat_types"] = base["repeat_types"].fillna(0)

    # Avg resolution days (from closed violations)
    closed_df = df[df["violationstatus"].str.upper() == "CLOSE"].copy()
    closed_df["days_to_resolve"] = (closed_df["closedate"] - closed_df["inspectiondate"]).dt.days
    closed_df = closed_df[closed_df["days_to_resolve"].between(0, 1825)]
    avg_res = closed_df.groupby("buildingid")["days_to_resolve"].mean().reset_index(name="avg_resolution_days")
    base = base.merge(avg_res, on="buildingid", how="left")
    base["avg_resolution_days"] = base["avg_resolution_days"].fillna(base["avg_resolution_days"].median())

    return base

def compute_health_score(df):
    def norm_inv(s):
        mn, mx = s.min(), s.max()
        if mx == mn:
            return pd.Series(50.0, index=s.index)
        return (1 - (s - mn) / (mx - mn)) * 100

    df["score_open"] = norm_inv(df["total_open"])
    df["score_severity"] = norm_inv(df["weighted_severity"])
    df["score_resolution"] = norm_inv(df["avg_resolution_days"])
    df["score_recency"] = norm_inv(df["recent_count"])
    df["score_repeat"] = norm_inv(df["repeat_types"])

    df["health_score"] = (
        df["score_open"] * 0.30
        + df["score_severity"] * 0.25
        + df["score_resolution"] * 0.20
        + df["score_recency"] * 0.15
        + df["score_repeat"] * 0.10
    ).round(1)

    bins = [0, 70, 80, 90, 100]
    labels = ["Poorly managed", "Struggling", "Moderate", "Well-managed"]
    df["label"] = pd.cut(df["health_score"], bins=bins, labels=labels, include_lowest=True)
    return df

def build_map(df):
    m = folium.Map(location=[40.72, -73.98], zoom_start=11, tiles="CartoDB positron")
    cluster = MarkerCluster(
        options={"maxClusterRadius": 40, "disableClusteringAtZoom": 15}
    ).add_to(m)

    color_map = {
        "Poorly managed": "#d73027",
        "Struggling": "#fc8d59",
        "Moderate": "#fee090",
        "Well-managed": "#4575b4",
    }

    for _, row in df.iterrows():
        label = str(row["label"])
        color = color_map.get(label, "#999999")
        address = f"{row['housenumber']} {row['streetname']}".strip()

        popup_html = f"""
        <div style="font-family: sans-serif; font-size: 13px; min-width: 200px;">
          <b>{address}</b><br>
          <span style="color:{color}; font-weight:bold;">&#9632; {label}</span><br>
          <hr style="margin:4px 0">
          Health Score: <b>{row['health_score']}</b>/100<br>
          Open violations: {int(row['total_open'])}<br>
          Class C (hazardous): {int(row['class_c'])}<br>
          Class B: {int(row['class_b'])}<br>
          Avg resolution: {row['avg_resolution_days']:.0f} days<br>
          Recent (6mo): {int(row['recent_count'])}
        </div>
        """

        folium.CircleMarker(
            location=[row["latitude"], row["longitude"]],
            radius=5,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.75,
            popup=folium.Popup(popup_html, max_width=260),
        ).add_to(cluster)

    # Legend
    legend_html = """
    <div style="position:fixed; bottom:30px; left:30px; z-index:1000;
                background:white; padding:12px 16px; border-radius:8px;
                box-shadow:0 2px 8px rgba(0,0,0,0.3); font-family:sans-serif; font-size:13px;">
      <b>Building Health Score</b><br>
      <span style="color:#d73027;">&#9632;</span> Poorly managed (&lt;70)<br>
      <span style="color:#fc8d59;">&#9632;</span> Struggling (70–80)<br>
      <span style="color:#fee090;">&#9632;</span> Moderate (80–90)<br>
      <span style="color:#4575b4;">&#9632;</span> Well-managed (90–100)
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

    return m

def main():
    print("Loading data...")
    df = load_and_clean(INPUT)
    print(f"  {len(df):,} rows with valid lat/lon")

    print("Aggregating by building...")
    bldg = aggregate_buildings(df)
    print(f"  {len(bldg):,} unique buildings")

    print("Computing health scores...")
    bldg = compute_health_score(bldg)
    print(bldg["label"].value_counts().sort_index())

    # Save scored table
    bldg.to_parquet(DATA_DIR / "buildings_scored.parquet", index=False)
    print("\nSaved → data/buildings_scored.parquet")

    # Top 20 worst
    worst = bldg.nsmallest(20, "health_score")[
        ["housenumber", "streetname", "boroid", "health_score", "label",
         "total_open", "class_c", "avg_resolution_days"]
    ]
    print("\n=== Top 20 Worst-Managed Buildings ===")
    print(worst.to_string(index=False))

    # Build and save map
    print("\nBuilding map...")
    m = build_map(bldg)
    m.save("daisy_prospect_map.html")
    print("Saved → daisy_prospect_map.html")

if __name__ == "__main__":
    main()
