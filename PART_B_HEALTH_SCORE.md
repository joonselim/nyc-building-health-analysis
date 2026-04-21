# Part B — Building Health Score

## Goal
Design and implement a composite score (0–100) for each NYC condo/co-op building that reflects how well the building is currently being managed.  
A low score = strong signal that the building needs a better management company = Daisy prospect.

---

## Why this matters for Daisy
Daisy's pitch to boards is: "your current management isn't doing enough — we do more, faster, with full transparency."  
The Building Health Score makes that pitch concrete. It quantifies exactly *how bad* things are, and lets Daisy say:  
*"Your building scores 24/100. Here's why. Here's what Daisy would change."*

This is a product feature Daisy doesn't currently have publicly — but could build into their sales or growth workflow.

---

## Score Components

| Component | Weight | Data Source | Logic |
|---|---|---|---|
| Open Violation Count | 30% | HPD Violations | More open violations = lower score |
| Violation Severity | 25% | HPD Violations | Class C weighted 3x, B weighted 2x, A weighted 1x |
| Resolution Speed | 20% | HPD Violations (closed) | Avg days to resolve closed violations — slower = lower score |
| Violation Recency | 15% | HPD Violations | More violations in last 6 months = lower score |
| Repeat Violation Rate | 10% | HPD Violations | Same violation type appearing multiple times = lower score |

Total: 100%

---

## Score Calculation

### Step 1 — Normalize each component to 0–100

For each building, compute raw values first, then normalize across all buildings using min-max scaling:

```python
from sklearn.preprocessing import MinMaxScaler
import numpy as np

def normalize_inverse(series):
    """Higher raw value = worse management = lower score"""
    scaler = MinMaxScaler()
    normalized = scaler.fit_transform(series.values.reshape(-1, 1)).flatten()
    return 1 - normalized  # invert so higher = better

building_stats['open_violation_score'] = normalize_inverse(building_stats['total_open_violations']) * 100
building_stats['severity_score'] = normalize_inverse(building_stats['weighted_severity']) * 100
building_stats['resolution_score'] = normalize_inverse(building_stats['avg_resolution_days'].fillna(building_stats['avg_resolution_days'].median())) * 100
building_stats['recency_score'] = normalize_inverse(building_stats['recent_violation_count']) * 100
building_stats['repeat_score'] = normalize_inverse(building_stats['repeat_violation_rate']) * 100
```

### Step 2 — Weighted severity calculation
```python
building_stats['weighted_severity'] = (
    building_stats['class_c_count'] * 3 +
    building_stats['class_b_count'] * 2 +
    building_stats['class_a_count'] * 1
)
```

### Step 3 — Recent violations (last 6 months)
```python
from datetime import datetime, timedelta

cutoff = datetime.now() - timedelta(days=180)
recent_df = open_df[pd.to_datetime(open_df['inspectiondate']) > cutoff]
recent_counts = recent_df.groupby('buildingid').size().reset_index(name='recent_violation_count')
building_stats = building_stats.merge(recent_counts, on='buildingid', how='left').fillna(0)
```

### Step 4 — Repeat violation rate
```python
# Count how many violation types appear more than once per building
repeat_df = open_df.groupby(['buildingid', 'novdescription']).size().reset_index(name='count')
repeat_df = repeat_df[repeat_df['count'] > 1]
repeat_counts = repeat_df.groupby('buildingid').size().reset_index(name='repeat_violation_rate')
building_stats = building_stats.merge(repeat_counts, on='buildingid', how='left').fillna(0)
```

### Step 5 — Composite score
```python
building_stats['health_score'] = (
    building_stats['open_violation_score'] * 0.30 +
    building_stats['severity_score'] * 0.25 +
    building_stats['resolution_score'] * 0.20 +
    building_stats['recency_score'] * 0.15 +
    building_stats['repeat_score'] * 0.10
).round(1)
```

---

## Score Interpretation

| Score | Label | What it means |
|---|---|---|
| 80–100 | Well-managed | Low violations, fast resolution — not a Daisy target right now |
| 60–79 | Moderate | Some open issues — worth monitoring |
| 40–59 | Struggling | Significant unresolved violations — warm prospect |
| 0–39 | Poorly managed | High volume, hazardous violations, slow response — **hot prospect** |

---

## Output Table

For each building, produce a row like:

| Building ID | Address | Borough | Health Score | Label | Open Violations | Class C Count | Avg Resolution (days) |
|---|---|---|---|---|---|---|---|
| 12345 | 145 W 86th St | Manhattan | 22 | Poorly managed | 47 | 12 | 94 |
| 67890 | 302 Atlantic Ave | Brooklyn | 61 | Moderate | 8 | 1 | 31 |

---

## Visualization (beyond the map)

After computing scores, also produce:

**1. Score distribution histogram**
```python
import matplotlib.pyplot as plt

plt.figure(figsize=(10, 5))
plt.hist(building_stats['health_score'], bins=20, color='steelblue', edgecolor='white')
plt.axvline(40, color='red', linestyle='--', label='Hot prospect threshold')
plt.xlabel('Building Health Score')
plt.ylabel('Number of Buildings')
plt.title('NYC Condo/Co-op Building Health Score Distribution')
plt.legend()
plt.savefig('score_distribution.png', dpi=150)
```

**2. Borough breakdown**
```python
borough_avg = building_stats.groupby('boroid')['health_score'].mean().reset_index()
borough_map = {'1': 'Manhattan', '2': 'Bronx', '3': 'Brooklyn', '4': 'Queens', '5': 'Staten Island'}
borough_avg['borough'] = borough_avg['boroid'].map(borough_map)

plt.figure(figsize=(8, 4))
plt.bar(borough_avg['borough'], borough_avg['health_score'], color='steelblue')
plt.title('Average Building Health Score by Borough')
plt.ylabel('Health Score (higher = better managed)')
plt.savefig('borough_breakdown.png', dpi=150)
```

**3. Top 20 worst-managed buildings table**
```python
worst = building_stats.nsmallest(20, 'health_score')[
    ['address', 'street', 'boroid', 'health_score', 'total_open_violations', 'class_c_count']
]
print(worst.to_markdown(index=False))
```

---

## Libraries needed
```
pip install pandas requests scikit-learn matplotlib folium
```

---

## Design notes
- Weights are intentionally explicit and adjustable — this is a v1 prototype, not a final model
- Score is relative (min-max normalized across the dataset), not absolute — document this clearly
- The score is descriptive, not predictive — it reflects current state, not future churn probability
- In a real product, Daisy could recalibrate weights based on which buildings actually converted
