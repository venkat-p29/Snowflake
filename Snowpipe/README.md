## Snowpipe for Event Driven Data Ingestion

1. Create an integration with GCP for storage access.
2. Create a stage that points to a specific GCS bucket.
3. Create a Pub/Sub topic. Publish to the topic when a file is uploaded in the bucket. Add Snowflake as a subscriber to the topic by creating an integration for topic access.
4. Create a Snowpipe. This automates by listening to the topic and runs COPY INTO table_name FROM @stage_name whenever a message is received from Pub/Sub topic. (this is done by the flag, auto_ingest = true)
5. Upload files into the bucket (via external processes).


#### How Snowflake Stage works:

When you create an external stage linked to GCS, the stage itself is essentially a pointer to the GCS bucket or path where your data files reside. However, when files are uploaded to GCS, they are not automatically loaded into Snowflake. The stage doesn't "create" corresponding files in Snowflake by itself; it's simply a reference to the location in GCS. When you upload files to the specified GCS path, they remain in GCS until you explicitly run a Snowflake command (like COPY INTO) to load them into a Snowflake table.  After files are uploaded into the GCS path, you would use the COPY INTO command to pull those files into a Snowflake table. Snowflake reads the files directly from GCS through the external stage.


## References:
    -Load data from GCS: https://docs.snowflake.com/en/user-guide/data-load-gcs-config
    -Snowpipe: https://www.chaosgenius.io/blog/snowflake-snowpipe/