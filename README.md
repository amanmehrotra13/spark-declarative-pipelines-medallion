# City Travels Medallion Architecture: High-Throughput Spark Declarative Pipelines

This project implements a production-grade **Medallion Architecture** using **Spark Declarative Pipelines (SDP)**. It demonstrates a modern shift from imperative ETL to a declarative data engineering model, focusing on **idempotency**, **cost-optimized incrementalism**, and **automated Change Data Capture (CDC)**.

---

## 🧠 Core Concept: Why Spark Declarative Pipelines?
Traditional Spark pipelines require manual management of checkpoints, state, and complex merge logic. This project leverages the **Enzyme Engine** within SDP to:
*   **Decouple Logic from Orchestration:** Focus on business transformations while the engine handles the dependency graph and state management.
*   **Incremental Join Management:** Unlike standard Spark, SDP Materialized Views allow for incremental updates on joined datasets, significantly reducing DPU consumption.
*   **Unified Governance:** Full integration with Unity Catalog for end-to-end lineage and attribute-based access control.

---

## 🏗️ Architectural Layers

### **1. Bronze Layer: Raw Ingestion**
*   **Pattern:** Streaming Tables with Auto Loader(`cloudFiles`).
*   **Strategy:** High-throughput, incremental ingestion of CSV-based Fact (Trips) and Reference (Cities) data from Cloud Volumes.
*   **Resilience:** Utilized `schemaEvolutionMode: rescue` to capture malformed records in a `_rescued_data` column without pipeline interruption, ensuring 100% data fidelity.

### **2. Silver Layer: Integration & Quality**
*   **Fact (Trips):** A Materialized View performing schema enforcement, data type casting, and column renaming for downstream compatibility.
*   **Dimension (Cities):** Implemented a **Slowly Changing Dimension (SCD Type 2)** using the `APPLY CHANGES INTO` syntax. 
    *   **Logic:** Tracks historical changes in city metadata (e.g., population/names) using `SEQUENCE BY` on file modification timestamps to ensure correct ordering.
*   **Data Quality:** Defined **Expectations** (`expect_or_drop`, `expect_or_fail`) as code decorators to enforce strict data contracts at the record level.

### **3. Gold Layer: Analytical Delivery**
*   **Pattern:** Materialized Views.
*   **Transformation:** Developed semantic views joining the Fact (Trips) with the **active version** of the Dimension (Cities).
*   **Optimization:** Pre-aggregated KPIs (Total Revenue, Trip Volume, Avg Ratings) designed for sub-second BI response times.

  ## *Catalog Setup*
<img width="956" height="395" alt="City_Travels_Catalog_Setup" src="https://github.com/user-attachments/assets/aa6bf5ea-996f-474a-be96-7297e8167ee6" />

  ## *Pipeline Lineage Graph*
<img width="929" height="415" alt="City_Travels_SDP" src="https://github.com/user-attachments/assets/b3ff1708-aa38-4ad7-b99b-2f591d9fef27" />

---

## 📊 Technical Excellence Highlights
*   **Incremental CDC:** Leveraged the Enzyme engine to update only changed records, avoiding the performance bottleneck of full table re-processing.
*   **Auditability:** Built-in tracking of `__start_at` and `__end_at` for every dimension change, providing a full point-in-time audit trail.
*   **Cost Efficiency:** Minimized compute footprint by utilizing event-driven triggers rather than persistent, always-on clusters.

---

## 🛠️ Tech Stack & Requirements
*   **Runtime:** Databricks Runtime 15.4+ (Spark 4.1+)
*   **Engine:** Spark SQL / PySpark (Declarative API)
*   **Governance:** Unity Catalog
*   **Storage:** Delta Lake

---

## 🚀 How to Deploy
1. Clone this repository into your **Databricks Workspace** using Git Folders.
2. Create a new **Lakeflow Pipeline** (Delta Live Tables).
3. Add the `pipelines/` (or `transformations/`) folder as the source code.
4. Set the `Target Schema` in Unity Catalog and click **Start**.
