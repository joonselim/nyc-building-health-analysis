"""
Build portfolio report: report.html
- Embeds PNG charts as base64 (self-contained)
- Embeds prospect map via iframe (daisy_prospect_map.html must be in same folder)
- Includes Top 20 table and methodology
"""
import base64
import pandas as pd
from pathlib import Path

DATA_DIR = Path("data")
BORO_MAP = {"1": "Manhattan", "2": "Bronx", "3": "Brooklyn", "4": "Queens", "5": "Staten Island"}

LABEL_ORDER = ["Poorly managed", "Struggling", "Moderate", "Well-managed"]
LABEL_COLORS = {
    "Poorly managed": "#d73027",
    "Struggling":     "#fc8d59",
    "Moderate":       "#ddb643",
    "Well-managed":   "#4575b4",
}


def b64_img(path):
    data = Path(path).read_bytes()
    return f"data:image/png;base64,{base64.b64encode(data).decode()}"


def build_top20_html(df):
    worst = df.nsmallest(20, "health_score")[[
        "housenumber", "streetname", "boroid", "health_score", "label",
        "total_open", "class_c", "avg_resolution_days", "recent_count"
    ]].copy()
    worst["borough"] = worst["boroid"].map(BORO_MAP)
    worst["avg_resolution_days"] = worst["avg_resolution_days"].round(0).astype(int)
    worst["recent_count"] = worst["recent_count"].astype(int)

    rows = ""
    for _, r in worst.iterrows():
        color = LABEL_COLORS.get(str(r["label"]), "#999")
        rows += f"""
        <tr>
          <td>{r['housenumber']} {r['streetname']}</td>
          <td>{r['borough']}</td>
          <td><b>{r['health_score']:.1f}</b></td>
          <td><span class="badge" style="background:{color}">{r['label']}</span></td>
          <td>{int(r['total_open'])}</td>
          <td>{int(r['class_c'])}</td>
          <td>{r['avg_resolution_days']}</td>
          <td>{int(r['recent_count'])}</td>
        </tr>"""

    return f"""
    <table>
      <thead><tr>
        <th>Address</th><th>Borough</th><th>Score</th><th>Label</th>
        <th>Open Violations</th><th>Class C</th><th>Avg Resolution (days)</th><th>Recent (6mo)</th>
      </tr></thead>
      <tbody>{rows}</tbody>
    </table>"""


def build_label_summary_html(df):
    counts = df["label"].value_counts().reindex(LABEL_ORDER).fillna(0).astype(int)
    total = counts.sum()
    rows = ""
    desc = {
        "Poorly managed":  "High violation volume, slow resolution — <b>hot Daisy prospects</b>",
        "Struggling":      "Significant unresolved violations — warm prospects worth monitoring",
        "Moderate":        "Some open issues, but not urgent",
        "Well-managed":    "Low violations, fast resolution — not a priority target right now",
    }
    for label in LABEL_ORDER:
        color = LABEL_COLORS[label]
        n = counts[label]
        pct = n / total * 100
        rows += f"""
        <tr>
          <td><span class="dot" style="background:{color}"></span> <b>{label}</b></td>
          <td style="text-align:right">{n:,}</td>
          <td style="text-align:right">{pct:.1f}%</td>
          <td>{desc[label]}</td>
        </tr>"""
    return f"""
    <table>
      <thead><tr><th>Label</th><th style="text-align:right">Buildings</th>
        <th style="text-align:right">Share</th><th>What it means</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>"""


def main():
    df = pd.read_parquet(DATA_DIR / "buildings_scored.parquet")
    df["label"] = pd.Categorical(df["label"], categories=LABEL_ORDER, ordered=True)

    n_buildings = len(df)
    n_hot = int((df["label"] == "Poorly managed").sum())
    n_warm = int((df["label"] == "Struggling").sum())
    n_open_viols = int(df["total_open"].sum())

    img_dist  = b64_img("score_distribution.png")
    img_boro  = b64_img("borough_breakdown.png")
    img_comp  = b64_img("score_components.png")

    top20_html   = build_top20_html(df)
    summary_html = build_label_summary_html(df)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Daisy Prospect Intelligence — Building Health Score</title>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; }}
    body {{
      margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: #f7f8fa; color: #1a1a2e; line-height: 1.6;
    }}
    .hero {{
      background: linear-gradient(135deg, #1a1a2e 0%, #16213e 60%, #0f3460 100%);
      color: #fff; padding: 60px 40px 48px; text-align: center;
    }}
    .hero h1 {{ margin: 0 0 8px; font-size: 2.2rem; letter-spacing: -0.5px; }}
    .hero .sub {{ font-size: 1.05rem; opacity: 0.75; margin-bottom: 28px; }}
    .kpi-row {{
      display: flex; justify-content: center; gap: 32px; flex-wrap: wrap; margin-top: 24px;
    }}
    .kpi {{
      background: rgba(255,255,255,0.1); border-radius: 12px;
      padding: 18px 28px; text-align: center; min-width: 140px;
    }}
    .kpi .num {{ font-size: 2rem; font-weight: 700; }}
    .kpi .lbl {{ font-size: 0.8rem; opacity: 0.7; margin-top: 2px; }}
    .container {{ max-width: 1100px; margin: 0 auto; padding: 40px 24px; }}
    .section {{ background: #fff; border-radius: 14px; padding: 32px 36px;
                margin-bottom: 32px; box-shadow: 0 2px 12px rgba(0,0,0,0.06); }}
    h2 {{ font-size: 1.4rem; margin: 0 0 6px; color: #0f3460; }}
    h3 {{ font-size: 1.05rem; margin: 24px 0 10px; color: #333; }}
    .section-note {{ font-size: 0.88rem; color: #666; margin: 0 0 20px; }}
    img.chart {{ width: 100%; border-radius: 8px; margin-top: 16px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 0.88rem; }}
    thead tr {{ background: #f0f2f5; }}
    th, td {{ padding: 9px 12px; text-align: left; border-bottom: 1px solid #eee; }}
    tbody tr:hover {{ background: #fafbfc; }}
    .badge {{
      display: inline-block; padding: 2px 9px; border-radius: 20px;
      color: #fff; font-size: 0.78rem; font-weight: 600; white-space: nowrap;
    }}
    .dot {{
      display: inline-block; width: 10px; height: 10px;
      border-radius: 50%; margin-right: 6px; vertical-align: middle;
    }}
    .map-frame {{
      width: 100%; height: 600px; border: none; border-radius: 10px;
      box-shadow: 0 2px 10px rgba(0,0,0,0.08);
    }}
    .method-grid {{
      display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 14px; margin-top: 16px;
    }}
    .method-card {{
      background: #f7f8fa; border-radius: 10px; padding: 14px 16px; border-left: 4px solid #0f3460;
    }}
    .method-card .weight {{ font-size: 1.3rem; font-weight: 700; color: #0f3460; }}
    .method-card .name {{ font-weight: 600; margin: 2px 0; font-size: 0.9rem; }}
    .method-card .desc {{ font-size: 0.8rem; color: #666; }}
    .caveat {{ background: #fff8e1; border-left: 4px solid #f9a825;
               padding: 14px 18px; border-radius: 8px; font-size: 0.88rem; margin-top: 12px; }}
    footer {{ text-align: center; padding: 32px; font-size: 0.82rem; color: #999; }}
  </style>
</head>
<body>

<div class="container">

  <!-- SUMMARY TABLE -->
  <div class="section">
    <h2>NYC Building Management Health Score</h2>
    <p>
      Each building receives a <b>Building Health Score (0–100)</b> based on
      <a href="https://data.cityofnewyork.us/Housing-Development/Housing-Maintenance-Code-Violations/wvxf-dwi5" target="_blank">NYC HPD (Housing Preservation and Development) public violation records</a>.
      A lower score means more unresolved violations, slower resolution, and more hazardous issues —
      signals that a building's current management company is underperforming.
      Across {n_buildings:,} buildings analyzed, there are currently <b>{n_open_viols:,} open violations</b> tracked.
    </p>
    <p class="section-note">{n_buildings:,} buildings with open HPD violations since Jan 2022 — scored on a 0–100 scale.<br>
      <a href="https://www.kaggle.com/code/joonselim/notebooked60569f34" target="_blank" style="font-size:0.85rem;">See in Kaggle →</a>
    </p>
    {summary_html}
  </div>

  <!-- INTERACTIVE MAP -->
  <div class="section">
    <h2>Building Health Score Map</h2>
    <p class="section-note">
      Each dot is a building. Color = health label. Click any dot to see address,
      score, violation breakdown, and average resolution time.
    </p>
    <iframe class="map-frame" src="daisy_prospect_map.html"></iframe>
  </div>

  <!-- TOP 20 -->
  <div class="section">
    <h2>Top 20 Worst-Managed Buildings</h2>
    <p class="section-note">
      Ranked by composite health score (lowest = highest priority for Daisy outreach).
    </p>
    {top20_html}
  </div>

  <!-- METHODOLOGY -->
  <div class="section">
    <h2>How the Score is Calculated</h2>
    <p class="section-note">
      Weighted composite of five components, each normalized 0–100 across the full dataset.
      Higher component score = better management on that dimension.
    </p>
    <div class="method-grid">
      <div class="method-card">
        <div class="weight">30%</div>
        <div class="name">Open Violation Count</div>
        <div class="desc">Total unresolved violations. More = lower score.</div>
      </div>
      <div class="method-card">
        <div class="weight">25%</div>
        <div class="name">Violation Severity</div>
        <div class="desc">Class C × 3, Class B × 2, Class A × 1. Weighted sum.</div>
      </div>
      <div class="method-card">
        <div class="weight">20%</div>
        <div class="name">Resolution Speed</div>
        <div class="desc">Avg days to close a violation. Slower = lower score.</div>
      </div>
      <div class="method-card">
        <div class="weight">15%</div>
        <div class="name">Recency</div>
        <div class="desc">Violations issued in the last 6 months. More recent = lower score.</div>
      </div>
      <div class="method-card">
        <div class="weight">10%</div>
        <div class="name">Repeat Violation Rate</div>
        <div class="desc">Same violation type appearing multiple times = lower score.</div>
      </div>
    </div>
  </div>

  <!-- SCORE DISTRIBUTION -->
  <div class="section">
    <h2>Score Distribution</h2>
    <p class="section-note">
      Labels use fixed score thresholds: &lt;70 = Poorly managed, 70–80 = Struggling, 80–90 = Moderate, 90–100 = Well-managed.
    </p>
    <img class="chart" src="{img_dist}" alt="Score distribution histogram">
  </div>

  <!-- BOROUGH BREAKDOWN -->
  <div class="section">
    <h2>Borough Breakdown</h2>
    <p class="section-note">Average health score and label mix by borough.</p>
    <img class="chart" src="{img_boro}" alt="Borough breakdown">
  </div>

  <!-- SCORE COMPONENTS -->
  <div class="section">
    <h2>Score Component Breakdown</h2>
    <p class="section-note">
      How each label group performs across the five scoring dimensions.
      Well-managed buildings score high on every component; poorly managed ones
      drag down especially on resolution speed and severity.
    </p>
    <img class="chart" src="{img_comp}" alt="Score components">
  </div>



</div>

<footer>
  Joonse Lim &nbsp;·&nbsp; joonselim@gmail.com &nbsp;·&nbsp;
  <a href="https://joonse.kr" target="_blank" style="color:#bbb">joonse.kr</a> &nbsp;·&nbsp;
  Data: <a href="https://data.cityofnewyork.us/Housing-Development/Housing-Maintenance-Code-Violations/wvxf-dwi5" target="_blank" style="color:#bbb">NYC Open Data HPD</a>
</footer>

</body>
</html>"""

    Path("report.html").write_text(html, encoding="utf-8")
    print("Saved → report.html")


if __name__ == "__main__":
    main()
