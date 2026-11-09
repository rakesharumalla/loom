-- Databricks SQL views over Lakebase CDF history tables (loom_catalog.loom_lakehouse.lb_*_history).
-- https://docs.databricks.com/aws/en/oltp/projects/lakebase-cdf
-- Applied by scripts/analytics_report.py; ${catalog} is substituted there.

CREATE OR REPLACE VIEW ${catalog}.loom_lakehouse.v_audit AS
SELECT ts, request_id, caller, surface, action, space_id, decision,
       get_json_object(detail, '$.reason') AS reason, detail
FROM ${catalog}.loom_lakehouse.lb_memory_audit_history WHERE _pg_change_type = 'insert';

CREATE OR REPLACE VIEW ${catalog}.loom_lakehouse.v_resolution_outcomes AS
SELECT outcome, rule, count(*) AS decisions, count(DISTINCT subject) AS subjects
FROM ${catalog}.loom_lakehouse.lb_conflict_decisions_history WHERE _pg_change_type = 'insert'
GROUP BY outcome, rule;

CREATE OR REPLACE VIEW ${catalog}.loom_lakehouse.v_space_usage_by_scope AS
SELECT CASE WHEN space_id = 'company' THEN 'company' ELSE split(space_id, ':')[0] END AS scope,
       decision, count(*) AS events, count(DISTINCT request_id) AS requests
FROM ${catalog}.loom_lakehouse.v_audit WHERE action = 'retrieve.space'
GROUP BY 1, 2;

CREATE OR REPLACE VIEW ${catalog}.loom_lakehouse.v_excluded_reasons AS
SELECT regexp_replace(reason, 'via .*', 'via ...') AS reason, count(*) AS exclusions
FROM ${catalog}.loom_lakehouse.v_audit WHERE action = 'retrieve.space' AND decision = 'excluded'
GROUP BY 1;

CREATE OR REPLACE VIEW ${catalog}.loom_lakehouse.v_low_rated_sources AS
SELECT source_ref, count(*) AS ratings, sum(CASE WHEN rating < 0 THEN 1 ELSE 0 END) AS negative,
       avg(rating) AS avg_rating
FROM ${catalog}.loom_lakehouse.lb_feedback_history WHERE _pg_change_type = 'insert'
GROUP BY source_ref;

CREATE OR REPLACE VIEW ${catalog}.loom_lakehouse.v_calls_by_surface AS
SELECT surface, action, count(DISTINCT request_id) AS requests
FROM ${catalog}.loom_lakehouse.v_audit WHERE action IN ('retrieve', 'contribute', 'configure')
GROUP BY surface, action;
