# NPO Impact Analytics

## Introduction
My brief stint as a student volunteer working at beach adoptions, special-needs homes and pediatric wards made me deeply aware of how essential nonprofit services are and how much they rely on  well-allocated funding. I often wondered how these organizations secured the resources needed for daily operations and how their “impact” was actually measured beyond the visible compassion and effort.

That curiosity became the foundation of this project. By analyzing nonprofit and grant data, I wanted to understand the relationship between funding and real-world outcomes, and whether data could highlight gaps, inequities, or opportunities to support high-impact organizations more effectively. 

At its core, the project aims to bridge field experience with data analytics to support nonprofits in securing the resources they need to continue their work.

![NGO Impact Dashboard](<img width="1500" height="1013" alt="NPO" src="https://github.com/user-attachments/assets/83841170-aa2d-4b15-899a-2179c8f7f5eb" />
)

## Overview
This project performs data‑driven analysis of nonprofit organizations and their grant allocations. Using SQL for data exploration and Plotly for interactive visualizations, the project uncovers funding patterns, organizational effectiveness, and underlying structures that influence impact scores.

## Objectives
- Understand grant distribution across nonprofits.
- Identify high‑impact organizations based on quantitative indicators.
- Detect patterns, clusters, or anomalies in performance.
- Build clear, interpretable visual summaries for decision‑makers.

## Data Description
1. grants.csv: Contains detailed information about publicly available grant opportunities.

2. non-profits.csv: Raw profile data of nonprofit organizations based on U.S. tax filings.

3. nonprofit_anomalies.csv :Outputs from an anomaly detection model identifying unusual patterns in nonprofit performance.
   
4. nonprofit_quality.csv: Measures the data completeness and quality of nonprofit records.

Dataset (synthetic nonprofit orgs and grants data) available on [Kaggle](https://www.kaggle.com/datasets/poojayakkala/nonprofit-organizations-and-grants-synthetic-data/data).


## Methodology
- SQL exploration for filtering, grouping, aggregation, and basic statistical checks.
- Plotly dashboards for interactive trend and distribution analysis.
- Report for actionable insights.


## Repository Structure
- notebooks/ — exploration and visualization  
- data/ — raw and cleaned data  
- reports/ — final insight report  
- README.md — project overview  
