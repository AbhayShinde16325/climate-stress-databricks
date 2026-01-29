# 🌍 Climate Stress Detection & Risk Prediction using Databricks

An end-to-end **Data Engineering + Machine Learning** project built on **Databricks Community Edition** to detect climate stress patterns and predict high climate risk states using historical temperature data.

This project demonstrates how raw climate data can be transformed into **actionable environmental intelligence** using scalable data architecture and interpretable ML models.

---

## 🧠 Problem Statement

Climate change signals are **relative, cumulative, and region-specific**.  
Simple threshold-based rules (for example, “temperature > X”) fail to capture long-term deviations and regional climate behavior.

**Objective:**
Build a scalable data and AI pipeline that:
- Identifies long-term temperature trends across regions
- Detects abnormal temperature deviations (climate stress signals)
- Predicts whether a region-time period represents a **high climate risk state**

---

## 📊 Dataset

**Source:** Global Climate Change Dataset (Kaggle)

**Files Used:**
- `GlobalTemperatures.csv`
- `GlobalLandTemperaturesByCountry.csv`
- `GlobalLandTemperaturesByState.csv`
- `GlobalLandTemperaturesByMajorCity.csv`

The datasets contain historical temperature measurements across time and geography.

---

## 🏗️ Architecture

This project follows **Medallion Architecture** using **Delta Lake**.

### 🥉 Bronze Layer (Raw Ingestion)
- Raw CSV files ingested using **Databricks Volumes**
- No transformations applied
- Stored as Delta tables

### 🥈 Silver Layer (Clean & Standardized)
- Date parsing and type normalization
- Standardized column naming
- Removal of structurally invalid rows
- Time-aware, analysis-ready datasets

### 🥇 Gold Layer (Analytics & ML Ready)
- Aggregated climate indicators
- Historical baselines per geography
- Temperature anomaly calculations
- Climate stress labels
- ML-ready feature tables

All layers use **Delta Lake** for ACID guarantees and reproducibility.

---

## ⚙️ Feature Engineering

Key engineered features include:
- Yearly average temperature
- Historical average temperature (baseline)
- Temperature anomaly (deviation from baseline)
- Rolling 12-month averages (city-level)
- Statistical climate stress indicator using standard deviation

These features enable **context-aware climate risk detection**.

---

## 🤖 Machine Learning

### Task
Binary Classification:
- `high_climate_risk` (1 = high risk, 0 = normal)

### Model
- **Logistic Regression**
  - Interpretable
  - Stable baseline
  - Suitable for policy and analytical use cases

### Features Used
- Year
- Average yearly temperature
- Historical average temperature
- Temperature anomaly

### Evaluation
- Metric: **AUC (Area Under ROC Curve)**
- Train/Test split: 80/20
- Experiments tracked using **MLflow**

---

## 🔁 End-to-End Workflow

1. Raw data ingested into Bronze Delta tables  
2. Cleaned and standardized into Silver tables  
3. Aggregated and feature-engineered into Gold tables  
4. ML model trained and evaluated  
5. Predictions persisted back into Delta tables  

This demonstrates a complete **Database ↔ AI ↔ Database** pipeline.

---

## 📈 Outputs

- Country-level climate risk indicators
- State-level climate risk indicators
- City-level climate risk indicators
- ML predictions stored as Delta tables
- Tracked ML experiments with MLflow

---

## 🌍 Business & Real-World Impact

- Early warning signals for policymakers and researchers
- Identification of regions experiencing abnormal climate stress
- Supports climate resilience and adaptation planning
- Framework is easily extendable to other climate variables

---

## ⚠️ Limitations

- Only temperature data is used (no precipitation, CO₂, etc.)
- Logistic Regression is a baseline model
- No future climate projection models included

---

## 🚀 Future Enhancements

- Add precipitation and greenhouse gas features
- Use temporal models (for example, tree-based or sequence models)
- Build SQL dashboards for interactive exploration
- Extend ML to state and city-level predictions

---

## 🧰 Tech Stack

- Databricks Community Edition
- PySpark & Spark SQL
- Delta Lake
- MLflow
- Python

---
🙌 Acknowledgements

Kaggle for the Global Climate Change dataset

Databricks Community Edition for the platform

---
