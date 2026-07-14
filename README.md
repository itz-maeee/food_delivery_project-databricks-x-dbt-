# food_delivery_project-databricks-x-dbt-
Built an end-to-end Food Delivery Analytics Platform using Databricks, PySpark, Delta Lake, Auto Loader, SQL, and dbt. Implemented the Medallion Architecture (Bronze, Silver, Gold) to automate data ingestion, perform ETL transformations, build analytics-ready models, and enforce data quality through dbt tests and documentation.


## 📌 Overview

The **Food Delivery Analytics Platform** is an end-to-end Data Engineering project built using **Databricks** and **dbt**. It demonstrates a modern data pipeline by implementing the **Medallion Architecture (Bronze, Silver, Gold)** to ingest, clean, transform, and model food delivery data for analytics.

The project uses **Databricks Auto Loader** for incremental data ingestion, **PySpark** for ETL transformations, **Delta Lake** for reliable storage, and **dbt** for SQL-based data modeling, testing, and documentation.

---

## 🚀 Tech Stack

* Databricks
* PySpark
* Delta Lake
* Databricks Auto Loader
* dbt Core
* SQL
* Git & GitHub

---

## 🏗️ Architecture

```text
Raw Data (CSV)
       │
       ▼
Databricks Auto Loader
       │
       ▼
Bronze Layer (Raw Delta Tables)
       │
       ▼
Silver Layer (Cleaned & Transformed)
       │
       ▼
dbt Models
       │
       ▼
Gold Layer (Analytics-ready Tables)
```

---

## 📂 Project Structure

```text
food-delivery-analytics/
│
├── data/
│   ├── landing/
│   └── sample_data/
│
├── notebooks/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   ├── marts/
│   │   └── schema.yml
│   ├── macros/
│   └── dbt_project.yml
│
├── README.md
└── requirements.txt
```

---

## 📊 Dataset

The project consists of:

* Orders
* Customers
* Restaurants
* Delivery Partners
* Payments
* Reviews

---

## ⚙️ Pipeline Workflow

### Bronze Layer

* Incrementally ingest raw CSV files using Databricks Auto Loader.
* Store data in Delta tables without modifications.

### Silver Layer

* Clean and validate data.
* Handle null values and duplicates.
* Standardize data types.
* Apply business rules using PySpark.

### Gold Layer

* Build analytics-ready fact and dimension tables using dbt.
* Implement dbt tests for data quality.
* Generate documentation and lineage.

---

## ✨ Key Features

* Incremental file ingestion with Auto Loader
* Medallion Architecture (Bronze, Silver, Gold)
* ETL using PySpark
* Delta Lake storage
* SQL-based transformations with dbt
* Data quality tests (unique, not null, relationships)
* dbt documentation and lineage

---

## 📈 Skills Demonstrated

* Data Engineering
* ETL Pipelines
* PySpark
* Databricks
* Delta Lake
* dbt
* SQL
* Data Modeling
* Data Quality Testing

---

## 📌 Future Enhancements

* Add streaming ingestion using Structured Streaming
* Implement Slowly Changing Dimensions (SCD)
* Schedule pipelines with Databricks Workflows
* Add CI/CD for dbt deployment

---

## 👨‍💻 Author

**Manaswee Balkawade**

If you found this project useful, feel free to ⭐ the repository.
