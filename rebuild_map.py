"""
Rebuild daisy_prospect_map.html with cluster color based on
AVERAGE health score of child markers (not marker count).

Fixes the misleading zoom-out view where the default Leaflet
MarkerCluster colors clusters red just because they contain
many markers — regardless of actual building health.

Reads: data/buildings_scored.parquet   (produced by part_a_map.py)
Writes: daisy_prospect_map.html
"""
import pandas as pd
import folium
from folium.plugins import MarkerCluster
from pathlib import Path

DATA_DIR = Path("data")
INPUT = DATA_DIR / "buildings_scored.parquet"
OUTPUT = Path("daisy_prospect_map.html")

COLOR_MAP = {
    "Poorly managed": "#d73027",
    "Struggling":     "#fc8d59",
    "Moderate":       "#fee090",
    "Well-managed":   "#4575b4",
}

# JS function run in the browser to create each cluster's icon.
# Uses the original Leaflet.markercluster default look (translucent outer
# ring + inner bubble), but picks the size-class based on the AVERAGE
# health score of child markers rather than the raw marker count.
ICON_CREATE_JS = r"""
function(cluster) {
    var children = cluster.getAllChildMarkers();
    var sum = 0, count = 0, poorCount = 0;
    children.forEach(function(m) {
        var cls = (m.options && m.options.className) || '';
        var match = cls.match(/hs-(-?[0-9.]+)/);
        if (match) {
            var score = parseFloat(match[1]);
            sum += score;
            count++;
            if (score < 70) poorCount++;
        }
    });
    var avg = count > 0 ? sum / count : 50;

    // Reuse Leaflet.markercluster default CSS (small=green, medium=yellow,
    // large=orange) and add one custom class for the rare "poorly managed"
    // tier. Mapping 4 score tiers onto these visual buckets:
    var sizeClass;
    if (avg < 70)      sizeClass = 'marker-cluster-poor';    // custom red
    else if (avg < 80) sizeClass = 'marker-cluster-large';   // default orange
    else if (avg < 90) sizeClass = 'marker-cluster-medium';  // default yellow
    else               sizeClass = 'marker-cluster-small';   // default green

    var n = children.length;
    var inner = '<div style="position:relative;"><span>' + n + '</span>';
    // Red dot badge: any poorly-managed building inside this cluster.
    // The per-cluster avg may still read green/yellow, but this makes
    // the rare "poor" buildings (0.1% of all) discoverable at any zoom.
    if (poorCount > 0) {
        inner += '<span class="hs-poor-badge" title="' + poorCount +
                 ' poorly managed building(s) inside"></span>';
    }
    inner += '</div>';

    return L.divIcon({
        html: inner,
        className: 'marker-cluster ' + sizeClass,
        iconSize: L.point(40, 40)
    });
}
"""

# Adds a "poorly managed" red tier on top of the three default tiers,
# matching the default visual language (translucent outer, solid inner).
CUSTOM_CLUSTER_CSS = """
<style>
.marker-cluster-poor {
    background-color: rgba(241, 128, 128, 0.6);
}
.marker-cluster-poor div {
    background-color: rgba(215, 48, 39, 0.75);
    color: white;
}
/* Red notification-style dot, shown when a cluster contains any
   poorly-managed building (score < 70). */
.hs-poor-badge {
    position: absolute;
    top: -3px;
    right: -3px;
    width: 11px;
    height: 11px;
    background: #d73027;
    border: 2px solid white;
    border-radius: 50%;
    box-shadow: 0 1px 2px rgba(0, 0, 0, 0.35);
    pointer-events: none;
}
</style>
"""


def build_map(df: pd.DataFrame) -> folium.Map:
    m = folium.Map(location=[40.72, -73.98], zoom_start=11, tiles="CartoDB positron")

    cluster = MarkerCluster(
        options={
            "maxClusterRadius": 40,
            "disableClusteringAtZoom": 15,
            "showCoverageOnHover": False,
            "spiderfyOnMaxZoom": True,
        },
        icon_create_function=ICON_CREATE_JS,
    ).add_to(m)

    for _, row in df.iterrows():
        label = str(row["label"])
        color = COLOR_MAP.get(label, "#999")
        score = float(row["health_score"])
        address = f"{row['housenumber']} {row['streetname']}".strip()

        popup_html = f"""
        <div style="font-family:sans-serif; font-size:13px; min-width:210px;">
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
            fill_opacity=0.78,
            weight=1,
            # Stash score into Leaflet options.className so the cluster
            # iconCreateFunction can read it.
            class_name=f"hs-{score:.1f}",
            popup=folium.Popup(popup_html, max_width=260),
        ).add_to(cluster)

    legend_html = """
    <div style="position:fixed; bottom:30px; left:30px; z-index:1000;
                background:white; padding:12px 16px; border-radius:8px;
                box-shadow:0 2px 8px rgba(0,0,0,0.3);
                font-family:sans-serif; font-size:13px; line-height:1.6;">
      <b>Building Health Score</b><br>
      <span style="color:#d73027;">&#9632;</span> Poorly managed (&lt;70)<br>
      <span style="color:#fc8d59;">&#9632;</span> Struggling (70–80)<br>
      <span style="color:#fee090;">&#9632;</span> Moderate (80–90)<br>
      <span style="color:#4575b4;">&#9632;</span> Well-managed (90–100)
      <div style="margin-top:6px; font-size:11px; color:#666; max-width:220px;">
        Cluster color = <i>average</i> score of buildings inside, not marker count.
      </div>
      <div style="margin-top:4px; font-size:11px; color:#666; display:flex; align-items:center; gap:6px;">
        <span style="display:inline-block; width:10px; height:10px; background:#d73027;
                     border:2px solid white; border-radius:50%;
                     box-shadow:0 1px 2px rgba(0,0,0,0.3);"></span>
        Red dot = cluster contains ≥1 poorly-managed building
      </div>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))
    m.get_root().html.add_child(folium.Element(CUSTOM_CLUSTER_CSS))
    return m


def main():
    print("Loading scored buildings...")
    df = pd.read_parquet(INPUT)
    print(f"  {len(df):,} buildings")
    print(df["label"].value_counts().sort_index())

    print("\nBuilding map...")
    m = build_map(df)
    m.save(str(OUTPUT))
    print(f"Saved → {OUTPUT}")


if __name__ == "__main__":
    main()
