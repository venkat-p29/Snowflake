## Data Ingestion from API To Snowflake

**Tech Stack**: Airflow, Google Cloud Storage, Python, Snowflake

1. Create an account in newsapi.org to get the API key.
2. Write a Python script which performs get api call, removes all null values and reduces the content column size to 200 characters.
3. Load the Polars dataframe into GCS Storage bucket as parquet file.
4. Create databases, integration and stage in Snowflake.
5. Create an Airflow script to orchestrate the tasks: Running Python script -> creating table in Snowflake -> Copying the parquet files loaded in the bucket into the created table.
6. Run the Airflow DAG in the Airflow Cloud Composer of GCP.


## References:
	-newspi.org