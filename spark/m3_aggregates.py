"""Spark SQL metrics over a validated enriched event view and covered_dates."""
def build(spark):
    owner=spark.sql('''SELECT event_date, split(repo_name,'/')[0] AS owner_name,
      'observed_owner_prefix' AS owner_classification, count(*) AS event_count,
      count(DISTINCT repo_id) AS repository_count, count(DISTINCT actor_id) AS active_actor_count,
      sum(CASE WHEN actor_id IS NULL THEN 1 ELSE 0 END) AS missing_actor_events,
      sum(CASE WHEN event_type='PushEvent' THEN 1 ELSE 0 END) AS push_events,
      sum(CASE WHEN event_type='PushEvent' THEN cast(get_json_object(payload_json,'$.size') AS BIGINT) END) AS declared_commit_count,
      sum(CASE WHEN event_type='PushEvent' AND get_json_object(payload_json,'$.size') IS NULL THEN 1 ELSE 0 END) AS missing_declared_commit_events,
      'COMPLETE' AS coverage FROM enriched_events GROUP BY event_date,split(repo_name,'/')[0]''')
    weekly=spark.sql('''WITH activity AS (
      SELECT cast(date_trunc('week',event_time) AS DATE) AS week_start, actor_id,
      count(*) AS event_count,count(DISTINCT event_date) AS active_days,count(DISTINCT repo_id) AS repository_count
      FROM enriched_events WHERE actor_id IS NOT NULL
      GROUP BY cast(date_trunc('week',event_time) AS DATE),actor_id
    ), coverage AS (
      SELECT cast(date_trunc('week',covered_date) AS DATE) AS week_start,count(*) AS covered_days
      FROM covered_dates GROUP BY cast(date_trunc('week',covered_date) AS DATE)
    ) SELECT a.*,c.covered_days,CASE WHEN c.covered_days=7 THEN 'COMPLETE' ELSE 'PARTIAL' END AS coverage
      FROM activity a JOIN coverage c ON a.week_start=c.week_start''')
    weekly.createOrReplaceTempView('weekly_activity')
    retention=spark.sql('''WITH first_seen AS (
      SELECT actor_id,min(week_start) AS cohort_week FROM weekly_activity GROUP BY actor_id
    ), cohorts AS (
      SELECT cohort_week,count(*) AS cohort_size FROM first_seen GROUP BY cohort_week
    ), retained AS (
      SELECT f.cohort_week,count(*) AS retained_count FROM first_seen f JOIN weekly_activity w
      ON f.actor_id=w.actor_id AND w.week_start=date_add(f.cohort_week,7) GROUP BY f.cohort_week
    ), coverage AS (
      SELECT cast(date_trunc('week',covered_date) AS DATE) AS week_start,count(*) AS covered_days
      FROM covered_dates GROUP BY cast(date_trunc('week',covered_date) AS DATE)
    ) SELECT c.cohort_week,date_add(c.cohort_week,7) AS observation_week,c.cohort_size,
      coalesce(cc.covered_days,0) AS cohort_covered_days,coalesce(oc.covered_days,0) AS observation_covered_days,
      CASE WHEN cc.covered_days=7 AND oc.covered_days=7 THEN coalesce(r.retained_count,0) END AS retained_count,
      CASE WHEN cc.covered_days=7 AND oc.covered_days=7 THEN cast(coalesce(r.retained_count,0) AS DOUBLE)/c.cohort_size END AS retention_rate,
      CASE WHEN cc.covered_days=7 AND oc.covered_days=7 THEN 'MATURE' ELSE 'IMMATURE' END AS maturity,
      'first_observed_in_selected_sample' AS cohort_definition
      FROM cohorts c LEFT JOIN retained r ON c.cohort_week=r.cohort_week
      LEFT JOIN coverage cc ON c.cohort_week=cc.week_start
      LEFT JOIN coverage oc ON date_add(c.cohort_week,7)=oc.week_start''')
    return {'dws_organisation_daily':owner,'dws_contributor_weekly':weekly,'ads_contributor_retention':retention}
