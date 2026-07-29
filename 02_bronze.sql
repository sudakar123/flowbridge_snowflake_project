-- Bronze layer: Create storage integration for Azure ADLS
-- Co-authored with CoCo
USE DATABASE flowbridge_dev_db;
USE ROLE ACCOUNTADMIN; 

--FLOWBRIDGE_DEV_DB.BRONZE_SCH.ADLS_RAW_STAGEFLOWBRIDGE_DEV_DB.BRONZE_SCH.ADLS_RAW_STAGEFLOWBRIDGE_DEV_DB.BRONZE_SCH.ADLS_RAW_STAGEDROP INTEGRATION IF EXISTS flowbridge_adls_integration;
CREATE STORAGE INTEGRATION IF NOT EXISTS flowbridge_adls_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'AZURE'
    ENABLED = TRUE
    AZURE_TENANT_ID = '91cabf99-9f71-494e-afed-64b3b4a0b3a9'
    STORAGE_ALLOWED_LOCATIONS = (
        'azure://flowbridgeproject.blob.core.windows.net/supply-chain-raw-dev/',
        'azure://flowbridgeproject.blob.core.windows.net/supply-chain_raw-prod/'
    );

SHOW INTEGRATIONS;

desc integration flowbridge_adls_integration

grant usage on  integration flowbridge_adls_integration to role sysadmin;


create or replace notification integration flowbridge_azure_notifications_int
    enabled = true
    type = queue
    notification_provider = azure_storage_queue
    azure_storage_queue_primary_uri = 'https://flowbridgeproject.queue.core.windows.net/flowbridge-supply-chain-queue'
    azure_tenant_id = '91cabf99-9f71-494e-afed-64b3b4a0b3a9';

desc integration flowbridge_azure_notifications_int;

grant usage on  integration flowbridge_azure_notifications_int to role sysadmin;

use role sysadmin;
use database flowbridge_dev_db;
use schema flowbridge_dev_db.bronze_sch;
use warehouse flowbridge_pipeline_wh

--file format
create file format if not exists bronze_sch.json_file_format
    type ='json'
    strip_outer_array = true
    comment ='JSON File Format for Flowbridge Project'

desc file format json_file_format;

--DROP STAGE IF EXISTS bronze_sch.adls_raw_stage;

--external stage
create stage if not exists bronze_sch.adls_raw_stage
    url = 'azure://flowbridgeproject.blob.core.windows.net/supply-chain-raw-dev/'
    storage_integration = flowbridge_adls_integration
    file_format = bronze_sch.json_file_format
    comment = 'External Stage - ADLS Gen2 DEV container';

list @bronze_sch.adls_raw_stage

create or replace transient table bronze_sch.raw_orders (
    raw_data variant,
    ingested_at timestamp_ntz default CURRENT_TIMESTAMP(),
    file_name string,
    file_row_number number,
    load_id string default uuid_string()
) comment = ' Bronze Layer - raw JSON supply chain orders for FlowBridge';



--snowpipe 
create pipe if not exists bronze_sch.supply_chain_pipe
    auto_ingest = true
    integration = flowbridge_azure_notifications_int
    comment = 'Snowpipe - auto ingest json files from ADLS Gen2'
as
copy into bronze_sch.raw_orders (
    raw_data,
    file_name,
    file_row_number
)
from (
    select $1,
           metadata$filename,
           metadata$file_row_number
    from @bronze_sch.adls_raw_stage
) file_format = (format_name = 'bronze_sch.json_file_format');

show pipes

select system$pipe_status('bronze_sch.supply_chain_pipe')

ALTER PIPE bronze_sch.supply_chain_pipe REFRESH;

select * from bronze_sch.raw_orders

select * from table( information_schema.copy_history(
        table_name => 'raw_orders',
        start_time => dateadd(hours,-1, current_timestamp())
    ));