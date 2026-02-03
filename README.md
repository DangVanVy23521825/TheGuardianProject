# TheGuardianProject

**End-to-End Data Engineering Pipeline for The Guardian**  
Automates news data ingestion, transformation, and analytics, with a demo chatbot for semantic retrieval.

## 🌟 Overview

TheGuardianProject is a complete **data engineering project** built using three core technologies: **Apache Airflow**, **dbt**, and **PostgreSQL**.  
The system automates the collection, transformation, and analytical modeling of news data from *The Guardian API*.  
In addition, the project includes a **demo semantic retrieval chatbot** to showcase how processed data can be queried using natural language.

## 🚀 Key Features

- **Data Ingestion**  
  Automatically fetches news articles from The Guardian API or sample data sources.

- **ETL & Data Transformation**  
  Uses **Airflow** to orchestrate data pipelines and **dbt** to clean, normalize, and transform raw data into analytics-ready models.

- **Analytical Data Storage**  
  Stores processed data in **PostgreSQL**, enabling downstream analytics via notebooks or BI tools.

- **Semantic Retrieval Chatbot (Demo)**  
  A lightweight chatbot that performs semantic search over the processed news content, demonstrating the integration of NLP with analytical data pipelines.

## 🛠️ Tech Stack

- **Apache Airflow** – Workflow orchestration for ETL pipelines  
- **dbt (data build tool)** – Data modeling and transformations  
- **PostgreSQL** – Relational database for analytical storage

## 📁 Repository Structure
├── airflow/            # Airflow DAGs and configurations
├── dbt/                # dbt project and transformation models
├── docker/             # PostgreSQL Docker setup
├── src/                # Pipeline scripts and processing jobs
├── requirements.txt    # Python dependencies
├── README.md           # Project documentation

## ⚡ Setup & Run Guide

### 1. Clone the repository

```bash
git clone https://github.com/DangVanVy23521825/TheGuardianProject.git
cd TheGuardianProject
```

### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 3. Start system components

- **PostgreSQL**
Install PostgreSQL or start it using Docker, then create a database for the project.
- **Apache Airflow**
Initialize and run Airflow services, and configure the connection to PostgreSQL.

```bash
airflow initdb
airflow webserver &
airflow scheduler &
```

- **dbt:**  
  Configure the database connection and execute transformations.

```bash
dbt run
```

- **Notebook & Chatbot:**  
Navigate to the notebook directory and open the provided notebooks in Jupyter to explore analytics or test the demo chatbot.

## 📣 Contributions

Contributions to improve the pipeline, add new features, or optimize performance are welcome.
Feel free to open an issue or submit a pull request.

**Author:** [DangVanVy23521825](https://github.com/DangVanVy23521825)
