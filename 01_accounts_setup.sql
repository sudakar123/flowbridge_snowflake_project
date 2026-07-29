create database if not exists flowbridge_dev_db
    comment = "Flowbridge supply chain- Development Database"

create schema if not exists flowbridge_dev_db.bronze_sch
    comment = "Raw Ingestion Layer";
create schema if not exists flowbridge_dev_db.silver_sch
    comment = "Transformation Layer";
create schema if not exists flowbridge_dev_db.gold_sch
    comment = "Aggeration Layer";
create schema if not exists flowbridge_dev_db.serving_sch
    comment = "Serving Layer"

create database if not exists flowbridge_prod_db
    comment = "Flowbridge supply chain- Production Database" 

create schema if not exists flowbridge_prod_db.bronze_sch
    comment = "Raw Ingestion Layer";
create schema if not exists flowbridge_prod_db.silver_sch
    comment = "Transformation Layer";
create schema if not exists flowbridge_prod_db.gold_sch
    comment = "Aggeration Layer";
create schema if not exists flowbridge_prod_db.serving_sch
    comment = "Serving Layer"

create warehouse if not exists flowbridge_pipeline_wh
    warehouse_size ='x-small'
    auto_suspend =60
    auto_resume = TRUE
    comment = 'pipeline workoads- Ingestion + transformation';

create warehouse if not exists flowbridge_analytics_wh
    warehouse_size ='x-small'
    auto_suspend =60
    auto_resume = TRUE
    comment = 'pipeline workoads- streamlit + data sharing'

-- Pipeline warehouse monitor
create or replace resource monitor flowbridge_pipeline_rm
    with credit_quota = 20
    frequency = monthly
    start_timestamp = immediately
    triggers
        on 75 percent do notify
        on 90 percent do notify
        on 100 percent do suspend;

-- Analytics warehouse monitor
create or replace resource monitor flowbridge_analytics_rm
    with credit_quota = 20
    frequency = monthly
    start_timestamp = immediately
    triggers
        on 75 percent do notify
        on 90 percent do notify
        on 100 percent do suspend;

alter warehouse flowbridge_pipeline_wh set resource_monitor = flowbridge_pipeline_rm;
alter warehouse flowbridge_analytics_wh set resource_monitor = flowbridge_analytics_rm;


use role accountadmin;

grant execute task on account to role sysadmin;

grant usage on warehouse flowbridge_pipeline_wh to role sysadmin;
grant usage on warehouse flowbridge_analytics_wh to role sysadmin;

grant all privileges on database flowbridge_dev_db to role sysadmin;
grant all privileges on database flowbridge_prod_db to role sysadmin;

grant all privileges on all schemas in database flowbridge_dev_db to role sysadmin;
grant all privileges on all schemas in database flowbridge_prod_db to role sysadmin;

show databases like 'flowbridge_%';
show warehouses like 'flowbridge_%';
show resource monitors;