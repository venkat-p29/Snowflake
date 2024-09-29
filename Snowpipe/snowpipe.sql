/*
This worksheet creates a Snowpipe Project connecting GCS bucket and Snowflake table for Event driven data ingestion

Reference: https://docs.snowflake.com/en/user-guide/data-load-gcs-config

*/

USE ROLE accountadmin;

CREATE OR REPLACE DATABASE snowpipe_demo;

CREATE OR REPLACE TABLE orders_data_lz(
    order_id int,
    product varchar(20),
    quantity int,
    order_status varchar(30),
    order_date date
);


-- Create a Cloud Storage Integration in Snowflake. Integration creates config based secure access

CREATE OR REPLACE STORAGE INTEGRATION gcs_bucket_read_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'GCS'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://gcs-snowflake-bucket/');

  
DESC STORAGE INTEGRATION gcs_bucket_read_int; 

-- Stage means reference to a specific external location where data will arrive

CREATE STAGE snowpipe_stage
  URL = 'gcs://gcs-snowflake-bucket/'
  STORAGE_INTEGRATION = gcs_bucket_read_int;


SHOW STAGES;

LIST @snowpipe_stage;


-- Create PUB-SUB Topic and Subscription. Run below command to create notification from GCS bucket to pub/sub topic (as publisher)
-- gsutil notification create -t snowpipe-pub-sub-topic -f json gs://gcs-snowflake-bucket/


-- Notification Integration to receive notifications (as subscriber) from GCP Pub-Sub topic

CREATE OR REPLACE NOTIFICATION INTEGRATION notification_from_pubsub_int
  ENABLED = TRUE
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = GCP_PUBSUB
  GCP_PUBSUB_SUBSCRIPTION_NAME = 'projects/marine-cycle-437102-k1/subscriptions/snowpipe-pub-sub-topic-sub';

DESC INTEGRATION notification_from_pubsub_int;


--Create Snowpipe
-- auto_ingest = true => This flag tells Snowpipe to automatically load files when a new notification is received from Pub/Sub

CREATE OR REPLACE PIPE gcs_to_snowflake_pipe
auto_ingest = true
INTEGRATION = notification_from_pubsub_int
AS
COPY INTO orders_data_lz
FROM @snowpipe_stage
file_format = (type = 'CSV');


-- Check the status of pipe
select system$pipe_status('gcs_to_snowflake_pipe');


-- Check the history of ingestion
Select * 
from table(information_schema.copy_history(table_name=>'orders_data_lz', start_time=> dateadd(hours, -1, current_timestamp())));

select * from orders_data_lz