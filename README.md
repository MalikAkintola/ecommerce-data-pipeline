# Olist E-Commerce Data Pipeline
An end-to-end data engineering project built in public — one hour a day, 14 days.

## Overview
This project builds a production-style data pipeline using the Olist Brazilian E-Commerce dataset from Kaggle. Raw data flows from Kaggle through AWS S3 into Snowflake, where it is transformed using dbt across a medallion architecture (bronze → silver → gold), and finally visualised in Power BI.

## Architecture
`Kaggle API → Python → AWS S3 → Snowflake (dbt) → Power BI`

## Stack
- **Python** — data ingestion & S3 upload (boto3)
- **AWS S3** — raw data storage
- **Snowflake** — data warehouse
- **dbt** — data transformation (medallion architecture)
- **Power BI** — analytics & dashboarding