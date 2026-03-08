# payroll-data-engineering-pipeline

This project demonstrates an end-to-end data engineering pipeline using the AWS ecosystem.

Payroll data stored as CSV in Amazon S3 is processed using AWS Glue (PySpark) 
and converted into Parquet format for efficient analytics.

The processed dataset is queried using Amazon Athena.

# Architecture

The pipeline follows this workflow:

1. Raw payroll CSV data stored in Amazon S3
2. AWS Glue (PySpark) processes and cleans the data
3. Data converted to parquet format for effecient analytics
4. Processed data stored in S3
5. Amazon Athena is used for querying the dataset

# Technologies Used

1. AWS S3
2. AWS Glue (Spark)
3. Amazon Athena
4. Apache Spark
5. Python

# Pipeline Steps
1. Upload raw payroll dataset to S3
2. Run Aws Glue ETL job
3. Convert CSV -> Parquet format
4. Store processed data in S3
5. Query data using Athena

