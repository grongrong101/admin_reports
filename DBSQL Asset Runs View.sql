-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## DBSQL All Assets History

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Use Lakeflow Table to Capture Number of Run Triggers and Total Trigger Duration (Simple & Recommended)

-- COMMAND ----------

SELECT 
  workspace_id,
  job_id,
  task_key,
  count(distinct run_id) as total_num_runs,
  result_state,
  sum(unix_timestamp(period_end_time) - unix_timestamp(period_start_time)) as total_run_duration_seconds,
  collect_list(distinct compute_ids) as warehouses
FROM system.lakeflow.job_task_run_timeline WHERE task_key IN ('refresh-dashboard', 'refresh-alert', 'refresh-query')
GROUP BY ALL
ORDER BY total_run_duration_seconds DESC


/* Possible to derive an estimate of cost, by taking total run duration and getting the warehouse cost (depending on size).*/

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Uses the Audit System Table to create a run history view then calculates additional metrics using the view. (Complex but captures additional fields)

-- COMMAND ----------

CREATE OR REPLACE VIEW jake_chen_ext.test.audit_dbsql_runs_view AS
WITH main AS (select 
      workspace_id, event_time, service_name, action_name,
      coalesce(from_json(response.result, 'struct<job_id:string>').job_id, request_params.job_id, request_params.jobId) as job_id,
      coalesce(request_params.multitaskParentRunId , request_params.runId) main_run_id,
      request_params.jobTaskType as task_type,
      request_params.taskKey as task_key,
      request_params.jobTriggerType as trigger_type,
      request_params.queryIds as query_id,
      request_params.dashboardId as dashboard_id,
      request_params.alertId as alert_id,
      request_params.warehouseId as warehouse_id,
      request_params.runCreatorUserName as creator_name,
      request_params,
      response,
      response.status_code as status,
      response.error_message as error
      from system.access.audit
      where service_name = 'jobs'
      and action_name in ('runTriggered','runFailed','runStart','runSucceeded')
      )
SELECT 
*
FROM main
WHERE workspace_id = 1444828305810485
-- and job_id in (598494025364008, 965584550167595, 380512730137397, 80712829573770, 734832363728344)
;

--create, schedule 1 min, schedule 5/4min, pause, then delete schedule
--query: 92b48da2-8c8a-41e9-913c-fd8758283112 380512730137397
--alert: f5664b9a-5721-46ca-8031-89261b9f099b 965584550167595
--dashboard: 78876dcb-21c6-4cf5-ae4a-39f297682fd6 598494025364008
--lakeview: 01ef5b6d5e201a9c9ee15da85eef00b6

-- COMMAND ----------

-- select * from jake_chen_ext.test.audit_dbsql_runs_view where job_id in (598494025364008, 965584550167595, 380512730137397, 80712829573770, 734832363728344)


-- COMMAND ----------

WITH trigger as ( select workspace_id, event_time, service_name, action_name, job_id, main_run_id, 
                         trigger_type 
                  from jake_chen_ext.test.audit_dbsql_runs_view where action_name in ('runTriggered')),
     run_start as ( select workspace_id, event_time, service_name, action_name, job_id, main_run_id, 
                           task_type, task_key, query_id, dashboard_id, alert_id, warehouse_id, request_params
                    from jake_chen_ext.test.audit_dbsql_runs_view where action_name in ('runStart')
                    and task_key in ('refresh-dashboard', 'refresh-alert', 'refresh-query') ),
     run_end as ( select workspace_id, event_time, service_name, action_name, job_id, main_run_id,
                         response, status, error
                  from jake_chen_ext.test.audit_dbsql_runs_view where action_name in ('runSucceeded','runFailed')
                  and task_key in ('refresh-dashboard', 'refresh-alert', 'refresh-query'))
SELECT t.workspace_id, 
       t.service_name,
       coalesce(re.action_name,t.action_name,t.action_name) as run_state,
       t.job_id, 
       t.main_run_id,
       t.trigger_type,
       t.event_time as triggered_event_time,
       r.event_time as run_event_time,
       re.event_time as run_end_event_time,
       unix_timestamp(re.event_time) - unix_timestamp(r.event_time) as run_duration_seconds,
       r.task_type, r.task_key, r.query_id, r.dashboard_id, r.alert_id, r.warehouse_id, r.request_params,
       re.response, re.status, re.error

FROM trigger t INNER JOIN run_start r ON t.job_id = r.job_id and t.main_run_id = r.main_run_id
          LEFT JOIN run_end re ON t.job_id = re.job_id and t.main_run_id = re.main_run_id
ORDER BY t.job_id, t.event_time

/* Possible to derive an estimate of cost, by taking run duration and getting the warehouse cost (depending on size).*/
