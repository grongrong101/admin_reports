-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## DBT Run Costs Tracker

-- COMMAND ----------

WITH raw_history AS (
SELECT
workspace_id,
statement_id,
executed_by,
statement_text,
compute.warehouse_id AS warehouse_id,
execution_status,
f.statement_type,
client_application,
COALESCE(error_message, 'None') AS error_message,
COALESCE(try_divide(total_duration_ms, 1000), 0) AS QueryRuntimeSeconds,
COALESCE(try_divide(total_task_duration_ms, 1000), 0) AS CPUTotalExecutionTime,
COALESCE(try_divide(execution_duration_ms, 1000), 0) AS ExecutionQueryTime,
COALESCE(try_divide(compilation_duration_ms, 1000), 0) AS CompilationQueryTime,
COALESCE(try_divide(waiting_at_capacity_duration_ms, 1000), 0) AS QueueQueryTime,
COALESCE(try_divide(waiting_for_compute_duration_ms, 1000), 0) AS StartUpQueryTime,
COALESCE(try_divide(result_fetch_duration_ms, 1000), 0) AS ResultFetchTime,
-- Metric for query cost allocation - -- exclude metadata operations
CASE WHEN COALESCE(try_divide(total_task_duration_ms, 1000),0) = 0 
    THEN 0 
    ELSE COALESCE(try_divide(total_duration_ms, 1000), 0)  + COALESCE(try_divide(compilation_duration_ms, 1000), 0) -- Query total time is compile time + execution time
    END AS TotalResourceTimeUsedForAllocation,
start_time,
end_time,
update_time,
COALESCE(read_bytes, 0) AS read_bytes,
COALESCE(read_io_cache_percent, 0) AS read_io_cache_percent,
from_result_cache,
COALESCE(spilled_local_bytes, 0) AS spilled_local_bytes,
COALESCE(total_task_duration_ms / total_duration_ms, NULL) AS TotalCPUTime_To_Execution_Time_Ratio, --execution time does seem to vary across query type, using total time to standardize
COALESCE(waiting_at_capacity_duration_ms / total_duration_ms, 0) AS ProportionQueueTime,
AVG(try_divide(total_duration_ms, 1000))  OVER () AS WarehouseAvgQueryRuntime,
AVG(try_divide(waiting_at_capacity_duration_ms, 1000)) OVER () AS WarehouseAvgQueueTime,
AVG(COALESCE(try_divide(waiting_at_capacity_duration_ms, 1000) / try_divide(total_duration_ms, 1000), 0)) OVER () AS WarehouseAvgProportionTimeQueueing,
-- Can use this to chargeback (as long as you know denominator is only USED task time, not including idele time)
CASE WHEN read_bytes > 0 THEN try_divide(read_bytes,(1024*1024*1024))ELSE 0 END AS ReadDataAmountInGB,
CASE WHEN read_io_cache_percent > 0 THEN 'Used Cache' ELSE 'No Cache' END AS UsedCacheFlag,
CASE WHEN spilled_local_bytes > 0 THEN 'Spilled Data To Disk' ELSE 'No Spill' END AS HasSpillFlag,
CASE WHEN read_bytes > 0 THEN 'Did Read Data' ELSE 'No Data Read' END AS ReadDataFlag,
CASE WHEN CPUTotalExecutionTime > 0 THEN 'UsedWorkerTasks' ELSE 'NoWorkers' END AS UsedWorkerTasksFlag,
CASE WHEN spilled_local_bytes > 0 THEN 1 ELSE 0 END AS Calc_HasSpillFlag,
CASE WHEN read_bytes > 0 THEN 0.25 ELSE 0 END AS Calc_ReadDataFlag,
CASE WHEN CPUTotalExecutionTime > 0 THEN 0.25 ELSE 0 END AS Calc_UsedWorkerTasksFlag,
substr(statement_text, INSTR(statement_text, '/*') + 2, INSTR(statement_text, '*/') - INSTR(statement_text, '/*') - 2) AS dbt_metadata_json
FROM system.query.history f
),
main_df as  (SELECT *, 
                    CASE WHEN (lower(dbt_metadata_json:app) = "dbt" OR client_application LIKE ('%dbt%')) THEN 'DBT Query' ELSE 'Other Query Type' END AS DBT_Query_Type,
                    COALESCE(dbt_metadata_json:app, 'None') AS app,
                    COALESCE(dbt_metadata_json:node_id, 'None') AS node_id,
                    COALESCE(dbt_metadata_json:profile_name, 'None') AS profile_name,
                    COALESCE(dbt_metadata_json:target_name, 'None') AS target_name,
                    COALESCE(dbt_metadata_json:connection_name, 'None') AS connection_name,
                    COALESCE(dbt_metadata_json:dbt_version, 'None') AS dbt_version,
                    COALESCE(dbt_metadata_json:dbt_databricks_version, 'None') AS dbt_databricks_version,
                    from_json(dbt_metadata_json, 'map<string,string>') AS parsed_dbt_comment,
                    COALESCE(dbt_metadata_json:node_id, dbt_metadata_json:connection_name, 'None') AS node_or_connection
            FROM raw_history),
 consumption_cost AS (
        SELECT
        u.usage_start_time,
        u.usage_end_time,
        l.sku_name,
        l.pricing.default AS sku_price,
        u.usage_date,
        u.usage_quantity,
        u.usage_metadata.warehouse_id,
        (l.pricing.default * u.usage_quantity) AS total_cost_for_warehouse_uptime
        FROM
        system.billing.list_prices l
        INNER JOIN system.billing.usage u ON l.sku_name = u.sku_name
        WHERE
        l.sku_name ILIKE '%serverless%sql%'
        ),
queries AS (
        SELECT
        start_time,
        end_time,
        executed_by,
        statement_id,
        execution_status,
        workspace_id,
        compute.warehouse_id as warehouse_id,
        statement_type
        FROM
        system.query.history
        ),
query_durations AS (
        SELECT
        q.statement_id,
        q.workspace_id,
        q.warehouse_id,
        q.start_time,
        q.end_time,
        COALESCE(TIMESTAMPDIFF(SECOND, GREATEST(q.start_time, c.usage_start_time), LEAST(q.end_time, c.usage_end_time)), 0) AS query_duration_in_interval,
        c.total_cost_for_warehouse_uptime / TIMESTAMPDIFF(SECOND, c.usage_start_time, c.usage_end_time) AS cost_per_second
        FROM
        queries q
        INNER JOIN consumption_cost c ON q.warehouse_id = c.warehouse_id
        WHERE
        q.start_time < c.usage_end_time AND q.end_time > c.usage_start_time
        ),
query_costs AS (
        SELECT
        q.statement_id,
        q.workspace_id,
        q.warehouse_id,
        q.start_time,
        q.end_time,
        SUM(qd.query_duration_in_interval * qd.cost_per_second) AS query_cost
        FROM
        queries q
        INNER JOIN query_durations qd ON q.statement_id = qd.statement_id
        GROUP BY
        q.statement_id,
        q.workspace_id,
        q.warehouse_id,
        q.start_time,
        q.end_time
        ),
query_cost AS (
        SELECT
        qc.statement_id,
        qc.workspace_id,
        qc.warehouse_id,
        qc.start_time,
        qc.end_time,
        qc.query_cost as apprx_query_cost,
        qq.total_task_duration_ms,
        qq.statement_text,
        qq.client_application
        FROM
        query_costs qc,
        system.query.history qq
        WHERE  
--        qc.workspace_id = 6589245167603355 AND 
        qc.start_time >= current_date() - INTERVAL 10 DAY
        AND qq.start_time >= current_date() - INTERVAL 10 DAY
        AND qc.statement_id = qq.statement_id)
SELECT  m.workspace_id, profile_name, target_name, node_or_connection,
        count(1) as num_runs,
        sum(CPUTotalExecutionTime) as sum_CPUTotalExecutionTime, 
        sum(QueryRuntimeSeconds) as sum_QueryRuntimeSeconds, 
        first(m.statement_id) as example_statement_id, 
        sum(apprx_query_cost) as sum_apprx_query_cost, 
        mean(apprx_query_cost) as avg_apprx_query_cost, 
        median(apprx_query_cost) as med_apprx_query_cost
FROM main_df m, query_cost qc
WHERE m.DBT_Query_Type = 'DBT Query' --THIS IS NECESSARY because sometimes it registers as DBSQL
AND m.QueryRuntimeSeconds > 60
AND m.workspace_id = qc.workspace_id
AND m.start_time >= current_date() - INTERVAL 10 DAY
AND m.statement_id = qc.statement_id
group by all
ORDER BY sum(CPUTotalExecutionTime) DESC
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Filter Runs by DBT Project

-- COMMAND ----------

WITH raw_history AS (
SELECT
workspace_id,
statement_id,
executed_by,
statement_text,
compute.warehouse_id AS warehouse_id,
execution_status,
f.statement_type,
client_application,
COALESCE(error_message, 'None') AS error_message,
COALESCE(try_divide(total_duration_ms, 1000), 0) AS QueryRuntimeSeconds,
COALESCE(try_divide(total_task_duration_ms, 1000), 0) AS CPUTotalExecutionTime,
COALESCE(try_divide(execution_duration_ms, 1000), 0) AS ExecutionQueryTime,
COALESCE(try_divide(compilation_duration_ms, 1000), 0) AS CompilationQueryTime,
COALESCE(try_divide(waiting_at_capacity_duration_ms, 1000), 0) AS QueueQueryTime,
COALESCE(try_divide(waiting_for_compute_duration_ms, 1000), 0) AS StartUpQueryTime,
COALESCE(try_divide(result_fetch_duration_ms, 1000), 0) AS ResultFetchTime,
-- Metric for query cost allocation - -- exclude metadata operations
CASE WHEN COALESCE(try_divide(total_task_duration_ms, 1000),0) = 0 
    THEN 0 
    ELSE COALESCE(try_divide(total_duration_ms, 1000), 0)  + COALESCE(try_divide(compilation_duration_ms, 1000), 0) -- Query total time is compile time + execution time
    END AS TotalResourceTimeUsedForAllocation,
start_time,
end_time,
update_time,
COALESCE(read_bytes, 0) AS read_bytes,
COALESCE(read_io_cache_percent, 0) AS read_io_cache_percent,
from_result_cache,
COALESCE(spilled_local_bytes, 0) AS spilled_local_bytes,
COALESCE(total_task_duration_ms / total_duration_ms, NULL) AS TotalCPUTime_To_Execution_Time_Ratio, --execution time does seem to vary across query type, using total time to standardize
COALESCE(waiting_at_capacity_duration_ms / total_duration_ms, 0) AS ProportionQueueTime,
AVG(try_divide(total_duration_ms, 1000))  OVER () AS WarehouseAvgQueryRuntime,
AVG(try_divide(waiting_at_capacity_duration_ms, 1000)) OVER () AS WarehouseAvgQueueTime,
AVG(COALESCE(try_divide(waiting_at_capacity_duration_ms, 1000) / try_divide(total_duration_ms, 1000), 0)) OVER () AS WarehouseAvgProportionTimeQueueing,
-- Can use this to chargeback (as long as you know denominator is only USED task time, not including idele time)
CASE WHEN read_bytes > 0 THEN try_divide(read_bytes,(1024*1024*1024))ELSE 0 END AS ReadDataAmountInGB,
CASE WHEN read_io_cache_percent > 0 THEN 'Used Cache' ELSE 'No Cache' END AS UsedCacheFlag,
CASE WHEN spilled_local_bytes > 0 THEN 'Spilled Data To Disk' ELSE 'No Spill' END AS HasSpillFlag,
CASE WHEN read_bytes > 0 THEN 'Did Read Data' ELSE 'No Data Read' END AS ReadDataFlag,
CASE WHEN CPUTotalExecutionTime > 0 THEN 'UsedWorkerTasks' ELSE 'NoWorkers' END AS UsedWorkerTasksFlag,
CASE WHEN spilled_local_bytes > 0 THEN 1 ELSE 0 END AS Calc_HasSpillFlag,
CASE WHEN read_bytes > 0 THEN 0.25 ELSE 0 END AS Calc_ReadDataFlag,
CASE WHEN CPUTotalExecutionTime > 0 THEN 0.25 ELSE 0 END AS Calc_UsedWorkerTasksFlag,
substr(statement_text, INSTR(statement_text, '/*') + 2, INSTR(statement_text, '*/') - INSTR(statement_text, '/*') - 2) AS dbt_metadata_json
FROM system.query.history f
),
main_df as  (SELECT *, 
                    CASE WHEN (lower(dbt_metadata_json:app) = "dbt" OR client_application LIKE ('%dbt%')) THEN 'DBT Query' ELSE 'Other Query Type' END AS DBT_Query_Type,
                    COALESCE(dbt_metadata_json:app, 'None') AS app,
                    COALESCE(dbt_metadata_json:node_id, 'None') AS node_id,
                    COALESCE(dbt_metadata_json:profile_name, 'None') AS profile_name,
                    COALESCE(dbt_metadata_json:target_name, 'None') AS target_name,
                    COALESCE(dbt_metadata_json:connection_name, 'None') AS connection_name,
                    COALESCE(dbt_metadata_json:dbt_version, 'None') AS dbt_version,
                    COALESCE(dbt_metadata_json:dbt_databricks_version, 'None') AS dbt_databricks_version,
                    from_json(dbt_metadata_json, 'map<string,string>') AS parsed_dbt_comment,
                    COALESCE(dbt_metadata_json:node_id, dbt_metadata_json:connection_name, 'None') AS node_or_connection
            FROM raw_history),
 consumption_cost AS (
        SELECT
        u.usage_start_time,
        u.usage_end_time,
        l.sku_name,
        l.pricing.default AS sku_price,
        u.usage_date,
        u.usage_quantity,
        u.usage_metadata.warehouse_id,
        (l.pricing.default * u.usage_quantity) AS total_cost_for_warehouse_uptime
        FROM
        system.billing.list_prices l
        INNER JOIN system.billing.usage u ON l.sku_name = u.sku_name
        WHERE
        l.sku_name ILIKE '%serverless%sql%'
        ),
queries AS (
        SELECT
        start_time,
        end_time,
        executed_by,
        statement_id,
        execution_status,
        workspace_id,
        compute.warehouse_id as warehouse_id,
        statement_type
        FROM
        system.query.history
        ),
query_durations AS (
        SELECT
        q.statement_id,
        q.workspace_id,
        q.warehouse_id,
        q.start_time,
        q.end_time,
        COALESCE(TIMESTAMPDIFF(SECOND, GREATEST(q.start_time, c.usage_start_time), LEAST(q.end_time, c.usage_end_time)), 0) AS query_duration_in_interval,
        c.total_cost_for_warehouse_uptime / TIMESTAMPDIFF(SECOND, c.usage_start_time, c.usage_end_time) AS cost_per_second
        FROM
        queries q
        INNER JOIN consumption_cost c ON q.warehouse_id = c.warehouse_id
        WHERE
        q.start_time < c.usage_end_time AND q.end_time > c.usage_start_time
        ),
query_costs AS (
        SELECT
        q.statement_id,
        q.workspace_id,
        q.warehouse_id,
        q.start_time,
        q.end_time,
        SUM(qd.query_duration_in_interval * qd.cost_per_second) AS query_cost
        FROM
        queries q
        INNER JOIN query_durations qd ON q.statement_id = qd.statement_id
        GROUP BY
        q.statement_id,
        q.workspace_id,
        q.warehouse_id,
        q.start_time,
        q.end_time
        ),
query_cost AS (
        SELECT
        qc.statement_id,
        qc.workspace_id,
        qc.warehouse_id,
        qc.start_time,
        qc.end_time,
        qc.query_cost as apprx_query_cost,
        qq.total_task_duration_ms,
        qq.statement_text,
        qq.client_application
        FROM
        query_costs qc,
        system.query.history qq
        WHERE  
--        qc.workspace_id = 6589245167603355 AND 
        qc.start_time >= current_date() - INTERVAL 10 DAY
        AND qq.start_time >= current_date() - INTERVAL 10 DAY
        AND qc.statement_id = qq.statement_id)
SELECT * 
FROM main_df m, query_cost qc
WHERE m.DBT_Query_Type = 'DBT Query' --THIS IS NECESSARY because sometimes it registers as DBSQL
AND m.QueryRuntimeSeconds > 1
AND m.workspace_id = qc.workspace_id
AND m.start_time >= current_date() - INTERVAL 10 DAY
AND m.statement_id = qc.statement_id
and m.node_or_connection = 'model.dai_subs_analytics.wbd_max_dimension_seamless_combination_base'
;
