-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## DBSQL All Assets History

-- COMMAND ----------

DECLARE OR REPLACE enhanced_query_history = '`bolt_infra_dev`.`dbx_observability`.`enhanced_query_history`';
DECLARE OR REPLACE dbsql_run_view = '`bolt_infra_dev`.`dbx_observability`.`audit_dbsql_runs_view`';
DECLARE OR REPLACE dbsql_schedule_all_view = '`bolt_infra_dev`.`dbx_observability`.`dbsql_schedule_history`';
DECLARE OR REPLACE dbsql_schedule_latest_view = '`bolt_infra_dev`.`dbx_observability`.`dbsql_schedule_view`';
DECLARE OR REPLACE lakeview_view = '`bolt_infra_dev`.`dbx_observability`.`lakeview_history`';


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Use Lakeflow Table to Capture Number of Run Triggers and Total Trigger Duration (Simple)

-- COMMAND ----------

SELECT 
  workspace_id,
  job_id,
  task_key,
  result_state,
  count(distinct run_id) as total_num_runs,
  sum(unix_timestamp(period_end_time) - unix_timestamp(period_start_time)) as total_run_duration_seconds,
  array_distinct(compute_ids) as warehouses
FROM system.lakeflow.job_task_run_timeline WHERE task_key IN ('refresh-dashboard', 'refresh-alert', 'refresh-query','sqlalert','sqldashboard','sqlsavedquery')
GROUP BY ALL
ORDER BY total_run_duration_seconds DESC


/* Possible to derive an estimate of cost, by taking total run duration and getting the warehouse cost (depending on size).*/

/* Pending Source in Query History to get Query level information.*/

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Uses the Audit System Table to create a run history view then calculates additional metrics using the view. (Complex but captures additional fields)

-- COMMAND ----------

CREATE OR REPLACE VIEW identifier(dbsql_run_view) AS
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
      ),
      trigger as ( select workspace_id, event_time, service_name, action_name, job_id, main_run_id, 
                         trigger_type 
                  from main where action_name in ('runTriggered')),
      run_start as ( select workspace_id, event_time, service_name, action_name, job_id, main_run_id, 
                           task_type, task_key, query_id, dashboard_id, alert_id, warehouse_id, request_params
                    from main where action_name in ('runStart')
                    and task_key in ('refresh-dashboard', 'refresh-alert', 'refresh-query','sqlalert', 'sqldashboard','sqlsavedquery') ),
      run_end as ( select workspace_id, event_time, service_name, action_name, job_id, main_run_id,
                         response, status, error
                  from main where action_name in ('runSucceeded','runFailed')
                  and task_key in ('refresh-dashboard', 'refresh-alert', 'refresh-query','sqlalert','sqldashboard','sqlsavedquery'))
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
FROM trigger t LEFT JOIN run_start r ON t.job_id = r.job_id and t.main_run_id = r.main_run_id
          LEFT JOIN run_end re ON t.job_id = re.job_id and t.main_run_id = re.main_run_id;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Here is a sample of aggregate the view to get metrics per job (i.e. num runs, run duration, etc)

-- COMMAND ----------

SELECT 
  workspace_id,
  job_id,
  task_key,
  run_state,
  query_id,
  dashboard_id,
  alert_id,
  count(distinct main_run_id) as total_num_runs,
  sum(unix_timestamp(run_end_event_time) - unix_timestamp(run_event_time)) as total_run_duration_seconds,
  collect_set(warehouse_id) as warehouses
FROM identifier(dbsql_run_view)
GROUP BY ALL
ORDER BY total_run_duration_seconds DESC

-- COMMAND ----------


