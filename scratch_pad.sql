-- Databricks notebook source
select * from `jake_chen_ext`.`test`.`enhanced_query_history`

-- COMMAND ----------

select * from system.compute.warehouse_events

-- COMMAND ----------

select * from system.billing.usage
where usage.sku_name ILIKE '%serverless%sql%'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC worspacename, id, dashboardname, owner, last execute, number of time executed

-- COMMAND ----------

SELECT
    *
FROM
   system.access.audit
WHERE service_name = 'dashboards' 
AND request_params.dashboard_id = '01ef5f4b66d615c9afd625bff89a0e04'

-- COMMAND ----------

SELECT
    *
FROM
   system.access.audit a
   inner join (
    select
      request_params.dashboard_id dashboard_id,
      last(a.user_Identity.email) publisher
    from
      system.access.audit a
    where
      event_Date >= "2023-01-01"  
      and action_name = 'publishDashboard'
    group by
      all
  ) d on a.request_params.dashboard_id = d.dashboard_id
-- WHERE a.action_name = 'executeQuery'

-- COMMAND ----------

SELECT * FROM `jake_chen_ext`.`test`.`lakeview_history`

-- COMMAND ----------


