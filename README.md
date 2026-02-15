# City Travels Medallion Architecture: High-Throughput Spark Declarative Pipelines

This project implements a production-grade **Medallion Architecture** using **Spark Declarative Pipelines (SDP)** formerly know as Delta Live Tables (DLT). It demonstrates a modern shift from imperative ETL to a declarative data engineering model, focusing on **idempotency**, **cost-optimized incrementalism**, and **automated Change Data Capture (CDC)**.

---

## 🧠 Core Concept: Why Spark Declarative Pipelines?
Traditional Spark pipelines require manual management of checkpoints, state, and complex merge logic. This project leverages the **Enzyme Engine** within SDP to:
*   **Decouple Logic from Orchestration:** Focus on business transformations while the engine handles the dependency graph and state management.
*   **Incremental Join Management:** Unlike standard Spark, SDP Materialized Views allow for incremental updates on joined datasets, significantly improving compute cost.
*   **Unified Governance:** Full integration with Unity Catalog for end-to-end lineage and attribute-based access control.

---

## 🏗️ Architectural Layers

### **1. Bronze Layer: Raw Ingestion**
*   **Pattern:** Streaming Tables with Auto Loader(`cloudFiles`).
*   **Strategy:** High-throughput, incremental ingestion of CSV-based Fact (Trips) and Reference (Cities) data from Cloud Volumes.
*   **Resilience:** Utilized `schemaEvolutionMode: rescue` to capture malformed records in a `_rescued_data` column without pipeline interruption, ensuring 100% data fidelity.

### **2. Silver Layer: Integration & Quality**
*   **Fact (Trips):** Streaming Table with data type casting, data quality checks, calculated columns, column renaming for downstream compatibility etc.
*   **Dimension (Cities):** Implemented a **Slowly Changing Dimension (SCD Type 2)** using the `APPLY CHANGES INTO` syntax. 
*   **Data Quality:** Defined **Expectations** (`expect_or_drop`, `expect_or_fail`) as code decorators to enforce strict data contracts at the record level.

### **3. Gold Layer: Analytical Delivery**
*   **Pattern:** Materialized Views.
*   **Transformation:** Developed semantic mviews joining the Fact (Trips) with the **active version** of the Dimension (Cities).
*   **Optimization:** Pre-aggregated KPIs (Total Revenue, Trip Volume, Avg Ratings) and a Detailed Granular Materialized view designed for sub-second BI response times with drill through.

  ## *High level Architecture Flow*
<img width="768" height="512" alt="High Level Architecture" src="https://github.com/user-attachments/assets/703c84ed-7dcd-408d-9e48-fd9eea88f6bf" />

  ## *Catalog Setup*
<img width="956" height="395" alt="City_Travels_Catalog_Setup" src="https://github.com/user-attachments/assets/aa6bf5ea-996f-474a-be96-7297e8167ee6" />

  ## *Pipeline Lineage Graph*
<img width="952" height="382" alt="image" src="https://github.com/user-attachments/assets/39c0ccd4-2e73-42be-be54-bbd0358bfff2" />


---

## 📊 Technical Excellence Highlights
*   **Incremental CDC:** Leveraged the Enzyme engine to update only changed records, avoiding the performance bottleneck of full table re-processing.
*   **Auditability:** Built-in tracking of `__start_at` and `__end_at` for every dimension change, providing a full point-in-time audit trail.
*   **Cost Efficiency:** Minimized compute footprint by utilizing event-driven triggers rather than persistent, always-on clusters.
*   **Performance:** Reduced compute overhead compared to traditional batch processing by replacing full-table overwrites with incremental Materialized View updates.

---

## 🛠️ Tech Stack & Requirements
*   **Runtime:** Databricks Runtime 15.4+ (Spark 4.1+)
*   **Engine:** Spark SQL / PySpark (Declarative API)
*   **Governance:** Unity Catalog
*   **Storage:** Delta Lake

---

## **💡 Implementation Strategy: Declarative vs. Imperative**
While the codebase is concise, it leverages the high-level **Spark Declarative Pipeline (SDP)** abstractions to handle complex distributed computing tasks and leaving the low-level orchestration, state management, and checkpointing to the SDP engine.

