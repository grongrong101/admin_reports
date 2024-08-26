-- Databricks notebook source
DECLARE OR REPLACE enhanced_query_history = '`bolt_infra_dev`.`dbx_observability`.`enhanced_query_history`';
DECLARE OR REPLACE dbsql_run_view = '`bolt_infra_dev`.`dbx_observability`.`audit_dbsql_runs_view`';
DECLARE OR REPLACE dbsql_schedule_all_view = '`bolt_infra_dev`.`dbx_observability`.`dbsql_schedule_history`';
DECLARE OR REPLACE dbsql_schedule_latest_view = '`bolt_infra_dev`.`dbx_observability`.`dbsql_schedule_view`';
DECLARE OR REPLACE lakeview_view = '`bolt_infra_dev`.`dbx_observability`.`lakeview_history`';


-- COMMAND ----------

CREATE OR REPLACE VIEW identifier(lakeview_view) as SELECT
    a.workspace_id,
    a.request_params.dashboard_id,
    a.request_id,
    h.statement_id,
    if( a.user_agent ilike "%lakeview_processing%", "scheduled", "interactive" ) mode,
    coalesce(d.publisher, h.executed_by) as user_owner,
    h.total_duration_ms/1000 as total_duration,
    h.waiting_for_compute_duration_ms/1000 as waiting_for_compute_duration,
    h.waiting_at_capacity_duration_ms/1000 as waiting_at_capacity_duration,
    h.execution_duration_ms/1000 as execution_duration,
    h.compilation_duration_ms/1000 as compilation_duration,
    h.total_task_duration_ms/1000 as total_task_duration,
    h.result_fetch_duration_ms/1000 as result_fetch_duration,
    cast(h.start_time as date) as start_date,
    cast(h.end_time as date) as end_date,
    cast(h.update_time as date) as update_date,
    a.action_name,
    a.event_date as event_date,
    h.*
    except (
    h.workspace_id, 
    h.statement_id, 
    h.executed_by, 
    h.total_duration_ms,
    h.waiting_for_compute_duration_ms,
    h.waiting_at_capacity_duration_ms,
    h.execution_duration_ms,
    h.compilation_duration_ms,
    h.total_task_duration_ms,
    h.result_fetch_duration_ms,
    h.start_time,
    h.end_time,
    h.update_time)
FROM
   system.access.audit a inner join system.query.history h on a.request_params.statement_id = h.statement_id
   left join (
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
WHERE a.action_name = 'executeQuery'

-- COMMAND ----------


