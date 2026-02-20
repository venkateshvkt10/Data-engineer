🏬 FMCG Sales Data Engineering Project – Decathlon
📌 Project Overview

This project demonstrates an end-to-end Data Engineering pipeline built using Databricks (Free Edition) in the FMCG domain.

The objective of this project is to design a scalable data model, implement ETL pipelines, and create analytical views to power business dashboards for sales, product, market, and customer insights.

The project simulates real-world retail analytics use cases for a company like Decathlon.

🏗 Architecture Overview

Raw Data → Bronze Layer → Silver Layer → Gold Layer → BI Dashboard

Bronze Layer – Raw data ingestion

Silver Layer – Cleaned & transformed data

Gold Layer – Business-ready fact & dimension tables

Analytics Layer – Enriched views for dashboarding

🛠 Technologies Used

Databricks (Free Edition)

PySpark

Spark SQL

Delta Lake

Star Schema Data Modeling

Git & GitHub

Power BI / Tableau (for reporting)

📊 Data Model

This project follows a Star Schema design:

🔹 Fact Table

fact_orders

date

product_code

customer_code

sold_quantity

🔹 Dimension Tables

dim_date

dim_customers

dim_products

dim_gross_price

🔹 Enriched Analytical View

vw_fact_orders_enriched

This view combines:

Date attributes (Year, Quarter, Month)

Customer attributes (Market, Platform, Channel)

Product attributes (Division, Category, Variant)

Metrics (Revenue, Quantity, Price)

📈 Business KPIs Implemented

Total Revenue

Total Quantity Sold

Distinct Customers

Distinct Products

Average Selling Price

Month-over-Month Growth

Year-over-Year Growth

Revenue by Market

Revenue by Category

Top 10 Products

Customer Pareto (80/20 Analysis)

🚀 Key Learnings

Designed scalable star schema data models

Built ETL pipelines using PySpark & SQL

Implemented Delta Lake architecture

Created analytical views for business reporting

Applied time intelligence concepts (MoM & YoY)

Developed executive-level KPI structures

🎯 Business Use Case

This project enables:

Sales performance tracking

Product performance analysis

Market contribution analysis

Customer segmentation insights

Executive-level dashboard reporting

📂 How to Run

Import data into Databricks workspace

Create Bronze tables

Apply transformations to build Silver tables

Create Gold fact & dimension tables

Build analytical view vw_fact_orders_enriched

Connect BI tool for visualization

🧠 Future Enhancements

Add incremental loading

Implement performance optimization

Add streaming data pipeline

Deploy using CI/CD

Build automated data quality checks

👨‍💻 Author

Venkatesh Vkt
Aspiring Data Engineer | PySpark | SQL | Databricks | Delta Lake
