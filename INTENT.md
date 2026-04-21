# Portfolio Project — Intent & Narrative

## Who I am
Joonse Lim — First-year MBA student at Duke Fuqua  
Background: software developer (Shinhan Bank), AI startup founder (secured government contract), product management  
Applying for: Product Operations internship at Daisy

## Why I'm building this
Daisy is not posting a standard internship application. They want people who move fast and contribute to real problems.  
Instead of submitting a resume and cover letter alone, I'm submitting a portfolio that shows I can think like a PM and actually build something useful.

The goal is not to impress with a fancy slide deck.  
The goal is to show: **I understand your business, I found a real problem, and I built a working prototype to address it.**

## What problem I'm addressing
Daisy grows by acquiring new buildings. A building switches management companies when the board decides the current company is not doing a good enough job.

The question is: **which buildings in NYC are most likely to be unhappy with their current management — and where are they?**

This information exists in public data. NYC's HPD (Department of Housing Preservation and Development) publishes daily-updated violation records for every residential building in the city. Buildings with high volumes of unresolved violations, slow resolution times, or repeated complaints are strong signals of poor management.

Nobody has built this into a usable sales/growth tool for a property management company.

## What I'm building
Two connected pieces:

**1. Daisy Prospect Map**  
An interactive map of NYC condo and co-op buildings, filtered by violation severity and recency.  
The map answers: *"Where should Daisy's sales team knock next?"*

**2. Building Health Score**  
A composite score per building based on HPD violation data.  
Inputs: number of open violations, violation class (A/B/C), average resolution time, 311 complaint volume, recency of violations.  
The score answers: *"How badly is this building being managed right now?"*

Together, these two pieces form a **lead prioritization tool** — something Daisy's growth or ops team could realistically use.

## What I want to demonstrate to Daisy

| What I show | What it signals to them |
|---|---|
| I used public data they already reference (HPD) | I understand their domain |
| I built something functional in Python | I can ship, not just talk |
| I framed it around their growth problem | I think like a PM, not an analyst |
| I documented my methodology | I communicate clearly |
| I connected the output to a real product feature idea | I can go from data → insight → product |

## What this is NOT
- This is not a strategy deck telling Daisy where to expand
- This is not a churn prediction model requiring internal data
- This is not a generic data science project

This is a **product prototype built from public data, framed around a specific business problem Daisy actually has.**

## Intended audience
- Hiring manager reading the portfolio link on my application
- Product or Ops team members who will interview me
- Anyone at Daisy who might actually want to use this tool
