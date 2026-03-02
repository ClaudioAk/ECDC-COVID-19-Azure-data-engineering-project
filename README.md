# 🦠 COVID-19 Data Engineering Pipeline 
# (Azure → Azure Databricks → SQL → Power BI/Fabric)

## 📌 Project Overview
This project showcases an **end-to-end Azure Data Engineering pipeline** that ingests public COVID-19 datasets, stores them in a raw/processed data lake structure, transforms them into analytics-ready formats, and serves them to an Azure SQL reporting layer for consumption in Power BI / Microsoft Fabric.

The solution follows common modeling best practices (dimension + fact style) to support scalable BI reporting and interactive dashboards.

## 🏗️ Architecture Overview

<img width="1376" height="768" alt="generated-image" src="https://github.com/user-attachments/assets/eb88413b-e9f6-40da-982b-9f49092de356" />

### 🔁 High-level flow
- 🌐 Ingest COVID-19 datasets from public sources (CSV/TSV) into Azure storage.
- 🗄️ Land raw data into **ADLS Gen2 (raw)**.
- ⚙️ Transform and cleanse using **Azure Databricks (PySpark)** and/or **ADF Mapping Data Flows**.
- 📦 Write curated outputs as **Parquet** into **ADLS Gen2 (processed)**.
- 🧱 Load analytics tables into **Azure SQL Database** using **ADF Copy activities**.
- 📊 Build a semantic model (with **DIM_COUNTRY**) and dashboards in **Power BI / Fabric**.

## 📊 Data Sources
- 🧾 ECDC datasets (CSV):
  - ✅ Cases & deaths
  - 🏥 Hospital admissions
  - 🧪 Testing
  - 👥 Cases by age
- 🧬 Population dataset (TSV)

## 🧰 Technologies Used
| Layer | Tools |
| --- | --- |
| 🧩 Orchestration | Azure Data Factory |
| 🗂️ Storage | Azure Data Lake Storage Gen2, Azure Blob Storage |
| 🔧 Transformation | Azure Databricks (PySpark), ADF Mapping Data Flows |
| 🏛️ Serving / Analytics | Azure SQL Database (fact tables + **DIM_COUNTRY**) |
| 📈 BI / Reporting | Power BI / Microsoft Fabric |
| 💻 Languages | PySpark, SQL, DAX |
| 🔀 Version Control | GitHub |


### 1️⃣ Ingestion (ADF)

<img width="1834" height="1079" alt="ADF_" src="https://github.com/user-attachments/assets/150c9d23-6e31-437d-831a-9b9ab133321a" />

- 🌐 Pulls multiple ECDC CSV datasets and lands them in ADLS Gen2 **raw** layer.
- 📥 Ingests the population TSV dataset from Blob Storage into the raw layer.

### 2️⃣ Transformation (Databricks / ADF)

<img width="1918" height="1079" alt="databricks_pyspark1" src="https://github.com/user-attachments/assets/be081b1f-f00f-4d07-880f-f9dad2a849a7" />

- 🧹 Standardizes schema and output format (Parquet).
- 🩹 Cleans common data quality issues (e.g., handling `:` as missing values in population fields).
- ✅ Outputs curated datasets to ADLS Gen2 **processed** layer.

### 3️⃣ Load to Azure SQL (ADF Copy)

- 🚚 Copies curated Parquet outputs into Azure SQL reporting tables.
- 🧩 Adds/maintains a simple dimension table (**DIM_COUNTRY**) to avoid many-to-many relationships in the BI model.

### 4️⃣ Reporting (Power BI / Fabric)

<img width="1314" height="741" alt="PowerBI" src="https://github.com/user-attachments/assets/22251ea3-1998-4fc1-9610-b49ff7971431" />

- 🔗 Connects Power BI to Azure SQL.
- 📊 Builds KPI cards, trends, breakdowns by age group, testing KPIs, and country slicers.

## 📁 Repository Structure
```bash
/adf/                 # ARM template or ADF publish artifacts (pipelines, datasets, linked services)
/databricks/          # notebooks (.py / .ipynb / .dbc export)
/sql/                 # schema.sql + DIM_COUNTRY script + optional views
/images/              # image(s)
/powerbi/             # PBIX (if allowed) + screenshots
README.md
```
🔐 Security Note
🔒 All secrets (keys, passwords, connection strings) are excluded from the repository and must be managed via secure mechanisms (Azure Key Vault / workspace secrets / parameters).

🚀 Future Enhancements
🤖 CI/CD automation for ADF + Databricks deployments.

⏩ Incremental loads and partitioning strategies.

✅ Data quality checks + monitoring/alerting.

🧠 Fabric-native version (Lakehouse + Semantic model + Direct Lake).

👤 Author
Claudio Akram Ayoub Mikhael
🔗 LinkedIn: www.linkedin.com/in/claudioak


