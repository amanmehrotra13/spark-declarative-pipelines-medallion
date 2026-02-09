# City Travels Medallion Architecture using Spark Declarative Pipeline

An end-to-end Databricks project demonstrating **Declarative Pipelines (SDP)**.

## 🚀 Architecture Overview
- **Bronze:** Incremental CSV ingestion using **Auto Loader** and Streaming Tables.
- **Silver:** 
    - **Trips:** Cleaned fact table with Data Quality Expectations.
    - **Cities:** Dimension table with **SCD Type 2** tracking via `APPLY CHANGES INTO`.
- **Gold:** Materialized Views providing pre-aggregated KPIs for BI reporting.

## 🛠️ Tech Stack
- **Engine:** Spark SQL / PySpark (Declarative API)
- **Governance:** Unity Catalog
- **Storage:** Delta Lake

## 📊 Key Features
- **Incremental Processing:** Uses the fast engine for cost-efficient updates.
- **Data Quality:** Integrated constraints (Warn, Drop, Fail) in the pipeline definition.
- **History Tracking:** Automated SCD2 logic to handle city metadata updates.
