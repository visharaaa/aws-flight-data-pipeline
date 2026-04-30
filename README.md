# AWS Flight Data ETL Pipeline

## Overview
This project implements an end-to-end data engineering pipeline on AWS to process and transform a flight price dataset for analytics and machine learning use cases.

The pipeline covers data ingestion, cleaning, transformation, feature engineering, and storage in a structured format for downstream consumption.

---

## Architecture

**Raw Data → AWS S3 (Raw Layer) → AWS Glue ETL (PySpark) → S3 (Processed Layer - Parquet)**

---

## Tech Stack
- AWS S3 (Data Lake storage)
- AWS Glue (ETL processing)
- PySpark (Data transformations)
- Python (Boto3 for ingestion)
- Parquet (Optimized storage format)

---

## Dataset
- Source: Kaggle Flight Price Dataset  
- Size: ~300,000+ records  
- Features include airline, route, duration, stops, class, and ticket price

---

## Pipeline Steps

### 1. Data Ingestion
- Raw CSV data is uploaded to Amazon S3 using a Python (Boto3) script
- Data stored in the **raw layer**

### 2. Data Cleaning (AWS Glue)
- Removed duplicate records
- Handled missing values (critical vs non-critical columns)
- Filtered invalid and corrupt records

### 3. Data Transformation
- Standardized categorical fields (airline, city names, etc.)
- Converted data types for numerical consistency
- Normalized “stops” field into structured format

### 4. Feature Engineering
- Average price per airline and class
- Average price per route (source → destination)

### 5. Output Layer
- Cleaned dataset stored in **Parquet format**
- Stored in S3 processed layer for efficient analytics and ML usage

---

## Key Features
- End-to-end AWS data pipeline
- Scalable ETL using AWS Glue + Spark
- Feature engineering for analytics readiness
- Structured data lake architecture (raw → processed)
- Optimized storage using Parquet format

---

## Tools & Services Used
- AWS S3
- AWS Glue
- AWS Glue Workflows
- PySpark
- Python (Boto3)

---

## Notes
- Built using AWS Free Tier resources
- Designed for batch processing (not streaming)
- Output dataset can be directly used for BI tools (e.g., Power BI) or ML models

---

## Author
Vishara Jayalath
