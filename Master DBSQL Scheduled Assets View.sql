-- Databricks notebook source
DECLARE OR REPLACE dbsql_schedule_audit_view = '`jake_chen_ext`.`test`.`dbsql_schedule_audit_view`';
DECLARE OR REPLACE dbsql_schedule_view = '`jake_chen_ext`.`test`.`dbsql_schedule_view`';


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DBSQL All Assets History

-- COMMAND ----------

CREATE OR REPLACE VIEW identifier(dbsql_schedule_audit_view) AS with main as (
  select
    workspace_id,
    event_time,
    service_name,
    action_name,
    user_identity,
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
    service_name = 'jobs'
)
select
  workspace_id,
  event_time,
  service_name,
  action_name,
  user_identity,
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

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## All DBSQL Assets Latest Schedules

-- COMMAND ----------

CREATE OR REPLACE VIEW identifier(dbsql_schedule_view) AS (
WITH audit_info as (
  SELECT
    *,
    dense_rank() OVER(
      PARTITION BY job_id
      ORDER BY
        event_time DESC
    ) as rec_rank
  from
    `jake_chen_ext`.`test`.`dbsql_schedule_audit_view`
),
schedule_view as (
  SELECT
    job_id,
    job_name,
      dashboard_id,
      warehouse_id,
      alert_id,
      query_id,
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
      `jake_chen_ext`.`test`.`dbsql_schedule_audit_view`
    WHERE
      schedule_cron is not null
  ),
  pause_view as (
    SELECT
      job_id,
      job_name,
      dashboard_id,
      warehouse_id,
      alert_id,
      query_id,
      schedule_cron,
      pause_status,
      event_time,
      dense_rank() OVER(
        PARTITION BY job_id
        ORDER BY
          event_time DESC
      ) as rec_rank
    FROM
      `jake_chen_ext`.`test`.`dbsql_schedule_audit_view`
    WHERE
      pause_status is not null
  ),
  create_view as (
    SELECT
      job_id,
      job_name,
      dashboard_id,
      warehouse_id,
      alert_id,
      query_id,
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
      `jake_chen_ext`.`test`.`dbsql_schedule_audit_view`
    WHERE
      schedule_info is not null
  )
  SELECT
    a.workspace_id,
    a.event_time as last_updated_time,
    a.service_name,
    a.user_identity,
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
    coalesce(
      a.job_name,
      s.job_name,
      c.job_name
    ) job_name,
    coalesce(
      a.dashboard_id,
      s.dashboard_id,
      c.dashboard_id
    ) dashboard_id,
    coalesce(
      a.alert_id,
      s.alert_id,
      c.alert_id
    ) alert_id,
    coalesce(
      a.query_id,
      s.query_id,
      c.query_id
    ) query_id,
    coalesce(
      a.warehouse_id,
      s.warehouse_id,
      c.warehouse_id
    ) warehouse_id,
    a.error,
    a.request_params,
    a.response
  FROM
    audit_info a
    LEFT JOIN schedule_view s ON a.job_id = s.job_id
    LEFT JOIN pause_view p ON a.job_id = p.job_id
    LEFT JOIN create_view c on a.job_id = c.job_id
  WHERE
    a.rec_rank = 1
    and s.rec_rank = 1
    and p.rec_rank = 1
    and c.rec_rank = 1
    AND action_name != 'delete'
    AND from_redash_ind = 'true')

-- COMMAND ----------

WITH job_info as (select * from identifier(dbsql_schedule_view)),
     run_info as (
      SELECT 
        workspace_id,
        job_id,
        max(triggered_event_time) as last_run_time,
        count(main_run_id) as total_num_runs,
        sum(run_duration_seconds) as total_run_duration_seconds
      FROM `jake_chen_ext`.`test`.`audit_dbsql_runs_view`
      GROUP BY ALL
     )
SELECT * FROM job_info left join run_info on job_info.job_id = run_info.job_id and job_info.workspace_id = run_info.workspace_id

-- COMMAND ----------

select * from identifier(dbsql_schedule_view) 

-- COMMAND ----------


