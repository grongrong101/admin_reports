-- Databricks notebook source
select * from identifier(:catalog);
select * from identifier(:schema);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DBSQL All Assets History

-- COMMAND ----------

CREATE
OR REPLACE VIEW ${catalog}.${schema}.audit_view AS with main as (
  select
    workspace_id,
    event_time,
    service_name,
    action_name,
    coalesce(
      from_json(response.result, 'struct<job_id:string>').job_id,
      request_params.job_id,
      request_params.jobId
    ) as job_id,
    --CREATE INFORMATION
    from_json(
      request_params.tasks,
      'array<struct<task_key:string, sql_task:struct<alert:struct<alert_id:string, pause_subscriptions:boolean>, warehouse_id:string>>>'
    ) AS create_info_alert,
    from_json(
      request_params.tasks,
      'array<struct<task_key:string, sql_task:struct<dashboard:struct<dashboard_id:string, pause_subscriptions:boolean>, warehouse_id:string>>>'
    ) AS create_info_dash,
    from_json(
      request_params.tasks,
      'array<struct<task_key:string, sql_task:struct<query:struct<query_id:string, pause_subscriptions:boolean>, warehouse_id:string>>>'
    ) AS create_info_query,
    --SCHEDULE INFORMATION
    from_json(
      request_params ['schedule'],
      'struct<quartz_cron_expression:string, timezone_id:string, pause_status:string>'
    ) AS schedule_info,
    --UPDATE INFORMATION
    from_json(
      request_params ['new_settings'],
      'struct<schedule:struct<quartz_cron_expression:string, timezone_id:string, pause_status:string>, tasks:array<struct<task_key:string, sql_task:struct<dashboard:struct<dashboard_id:string, pause_subscriptions:boolean>, warehouse_id:string>>>>'
    ) AS update_info,
    request_params.name as job_name,
    --only for creates
    request_params.is_from_redash as from_redash_ind,
    response.status_code as status,
    response.error_message as error,
    request_params,
    response
  from
    system.access.audit
  where
    service_name = 'jobs' and workspace_id = 1444828305810485
)
select
  workspace_id,
  event_time,
  service_name,
  action_name,
  coalesce(
    schedule_info.quartz_cron_expression,
    update_info.schedule.quartz_cron_expression
  ) as schedule_cron,
  schedule_info,
  coalesce(
    schedule_info.pause_status,
    update_info.schedule.pause_status
  ) as pause_status,
  status,
  from_redash_ind,
  job_id,
  job_name,
  create_info_alert.sql_task.alert.alert_id [0] as alert_id,
  create_info_dash.sql_task.dashboard.dashboard_id [0] as dashboard_id,
  create_info_query.sql_task.query.query_id [0] as query_id,
  coalesce(
    update_info.tasks.sql_task.warehouse_id [0],
    create_info_alert.sql_task.warehouse_id [0],
    create_info_dash.sql_task.warehouse_id [0],
    create_info_query.sql_task.warehouse_id [0]
  ) as warehouse_id,
  error,
  request_params,
  update_info,
  response
from
  main
where
  action_name in ('create', 'update', 'delete')
order by
  job_id,
  event_time;
--For Internal Testing Purposes
  --create, schedule 1 min, schedule 5/4min, pause, then delete schedule
  --query: 92b48da2-8c8a-41e9-913c-fd8758283112 380512730137397
  --alert: f5664b9a-5721-46ca-8031-89261b9f099b 965584550167595
  --dashboard: 78876dcb-21c6-4cf5-ae4a-39f297682fd6 598494025364008
  --lakeview: 01ef5b6d5e201a9c9ee15da85eef00b6

-- COMMAND ----------

select * from ${catalog}.${schema}.audit_view

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## All DBSQL Assets Latest Schedules

-- COMMAND ----------

WITH audit_info as (
  SELECT
    *,
    dense_rank() OVER(
      PARTITION BY job_id
      ORDER BY
        event_time DESC
    ) as rec_rank
  from
    ${catalog}.${schema}.audit_view
),
schedule_view as (
  SELECT
    job_id,
    schedule_cron,
    schedule_info,
    pause_status,
    event_time,
    dense_rank() OVER(
      PARTITION BY job_id
      ORDER BY
        event_time DESC
    ) as rec_rank
  FROM
    ${catalog}.${schema}.audit_view
  WHERE
    schedule_cron is not null
),
pause_view as (
  SELECT
    job_id,
    schedule_cron,
    pause_status,
    event_time,
    dense_rank() OVER(
      PARTITION BY job_id
      ORDER BY
        event_time DESC
    ) as rec_rank
  FROM
    ${catalog}.${schema}.audit_view
  WHERE
    pause_status is not null
)
SELECT
  a.workspace_id,
  a.event_time as last_updated_time,
  a.service_name,
  a.action_name as last_updated_action,
  coalesce(a.schedule_cron, s.schedule_cron) schedule_cron,
  coalesce(a.pause_status, s.pause_status, p.pause_status) pause_status,
  coalesce(
    a.schedule_info,
    s.schedule_info,
    a.update_info.schedule
  ) schedule_info,
  a.status,
  a.from_redash_ind,
  a.job_id,
  a.job_name,
  a.alert_id,
  a.dashboard_id,
  a.query_id,
  a.warehouse_id,
  a.error,
  a.request_params,
  a.response
FROM
  audit_info a
  LEFT JOIN schedule_view s ON a.job_id = s.job_id
  LEFT JOIN pause_view p ON a.job_id = p.job_id
WHERE
  a.rec_rank = 1
  and s.rec_rank = 1
  and p.rec_rank = 1
  AND action_name != 'delete'
  AND from_redash_ind = 'true'
