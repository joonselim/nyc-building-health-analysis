# Part A — Daisy Prospect Map

## Goal
Build an interactive map of NYC condo and co-op buildings that are likely candidates for switching to a new property management company, based on HPD violation data.

## Output
An interactive HTML map (Folium or Plotly) where each dot is a building.  
Color = health signal (red = poor management, green = well-managed).  
Click on a dot → see building address, violation count, top violation types, score.

---

## Data Sources

### Primary
**NYC Open Data — HPD Housing Maintenance Code Violations**  
URL: https://data.cityofnewyork.us/Housing-Development/Housing-Maintenance-Code-Violations/wvxf-dwi5  
API endpoint: https://data.cityofnewyork.us/resource/wvxf-dwi5.json  
Key fields:
- `buildingid` — unique building identifier
- `boroid`, `block`, `lot` — BBL (borough-block-lot) for joining
- `violationclass` — A (non-hazardous), B (hazardous), C (immediately hazardous)
- `inspectiondate` — when violation was issued
- `currentstatus` — open vs closed
- `novdescription` — what the violation is for
- `address`, `latitude`, `longitude` — location

### Secondary (for filtering to condo/co-op only)
**NYC Open Data — HPD Building Registration**  
URL: https://data.cityofnewyork.us/Housing-Development/Multiple-Dwelling-Registrations/tesw-yqqr  
Key fields:
- `buildingid`
- `communitydistrict`
- `buildingclassification` — use to filter for condo/co-op types

---

## Step-by-Step Build Plan

### Step 1 — Pull violation data via API
```python
import requests
import pandas as pd

# Pull recent violations (last 2 years) with lat/lon
url = "https://data.cityofnewyork.us/resource/wvxf-dwi5.json"
params = {
    "$limit": 100000,
    "$where": "inspectiondate > '2023-01-01'",
    "$select": "buildingid, violationclass, currentstatus, inspectiondate, novdescription, latitude, longitude, housenumber, streetname, boroid"
}
response = requests.get(url, params=params)
df = pd.DataFrame(response.json())
```

### Step 2 — Filter to open violations only
```python
# Open violations = current management has not resolved them
open_df = df[df['currentstatus'].str.contains('Open', case=False, na=False)]
```

### Step 3 — Aggregate by building
```python
building_stats = open_df.groupby('buildingid').agg(
    total_open_violations=('violationclass', 'count'),
    class_c_count=('violationclass', lambda x: (x == 'C').sum()),
    class_b_count=('violationclass', lambda x: (x == 'B').sum()),
    class_a_count=('violationclass', lambda x: (x == 'A').sum()),
    latitude=('latitude', 'first'),
    longitude=('longitude', 'first'),
    address=('housenumber', 'first'),
    street=('streetname', 'first'),
).reset_index()
```

### Step 4 — Filter to condo/co-op buildings
Join with HPD Building Registration data on `buildingid`.  
Filter by building classification codes for condo/co-op.  
If classification join is incomplete, use a proxy: buildings with 10+ units and violation data in Manhattan/Brooklyn are likely condo/co-op targets.

### Step 5 — Calculate resolution time for closed violations
```python
# Pull closed violations too, calculate days to resolve
closed_df = df[df['currentstatus'].str.contains('Close', case=False, na=False)]
closed_df['inspectiondate'] = pd.to_datetime(closed_df['inspectiondate'])
closed_df['closedate'] = pd.to_datetime(closed_df['closedate'])
closed_df['days_to_resolve'] = (closed_df['closedate'] - closed_df['inspectiondate']).dt.days

avg_resolution = closed_df.groupby('buildingid')['days_to_resolve'].mean().reset_index()
avg_resolution.columns = ['buildingid', 'avg_resolution_days']
```

### Step 6 — Build the map
```python
import folium
from folium.plugins import MarkerCluster

m = folium.Map(location=[40.7128, -74.0060], zoom_start=12)
cluster = MarkerCluster().add_to(m)

for _, row in building_stats.iterrows():
    if pd.isna(row['latitude']): continue
    
    color = 'red' if row['class_c_count'] > 5 else \
            'orange' if row['total_open_violations'] > 20 else 'green'
    
    popup_text = f"""
    <b>{row['address']} {row['street']}</b><br>
    Open violations: {row['total_open_violations']}<br>
    Class C (hazardous): {row['class_c_count']}<br>
    Health Score: {row['health_score']}
    """
    
    folium.CircleMarker(
        location=[float(row['latitude']), float(row['longitude'])],
        radius=6,
        color=color,
        fill=True,
        popup=folium.Popup(popup_text, max_width=250)
    ).add_to(cluster)

m.save('daisy_prospect_map.html')
```

---

## Expected Output
- Interactive HTML map, openable in any browser
- ~500–2,000 buildings plotted across NYC (filtered to condo/co-op)
- Color-coded by urgency
- Clickable popups with building-level detail
- Exportable as a standalone file for portfolio

---

## Libraries needed
```
pip install pandas requests folium
```

---

## Notes
- NYC Open Data API allows up to 50,000 rows per call without authentication; use `$offset` for pagination if needed
- Latitude/longitude is not always present — drop rows where missing
- Building classification filter: look for codes starting with 'D' (elevator apartments) or 'C' (walk-up apartments) in the DOF classification system
- If API is slow, download CSV directly from the NYC Open Data portal and load locally
