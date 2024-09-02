# aws-es-migration

Ingesting a 2 million records (2 GB) dataset from Kaggle into an AWS EC2 instance using the kaggle API


This is then sent to a PostgreSQL database known as AWS RDS using a spark job for faster processing.


The dataset ingested to RDS is then transferred to Elasticsearch and is stored as documents


3 python functions for updating , deleting and inserting records into elasticsearch are stoered in S3 and then imported as lambda functions
These lambda functions are scheduled to run in different intervals using AWS EventBridge


AWS CloudWatch monitors the logs of these lambda functions


