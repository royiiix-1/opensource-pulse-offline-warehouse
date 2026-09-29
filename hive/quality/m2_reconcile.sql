-- Return mismatched rows. Counts are independently derived from persisted DWD facts.
SELECT coalesce(d.repo_id,s.repo_id) repo_id
FROM (SELECT repo_id,count(*) n,count(distinct actor_id) actors FROM dwd_event GROUP BY repo_id) d
FULL OUTER JOIN dws_repository_daily s ON d.repo_id=s.repo_id
WHERE d.n IS NULL OR s.event_count IS NULL OR d.n<>s.event_count OR d.actors<>s.active_actors;

SELECT coalesce(p.repo_id,s.repo_id) repo_id
FROM (SELECT repo_id,count(*) n FROM dwd_push_event GROUP BY repo_id) p
FULL OUTER JOIN dws_repository_daily s ON p.repo_id=s.repo_id
WHERE coalesce(p.n,0)<>coalesce(s.push_events,0);

SELECT coalesce(p.repo_id,s.repo_id) repo_id
FROM (SELECT repo_id,
 count(CASE WHEN action='opened' THEN 1 END) opened,
 count(CASE WHEN action='closed' THEN 1 END) closed,
 count(CASE WHEN action='closed' AND merged THEN 1 END) merged
 FROM dwd_pull_request_event GROUP BY repo_id) p
FULL OUTER JOIN dws_repository_daily s ON p.repo_id=s.repo_id
WHERE coalesce(p.opened,0)<>coalesce(s.pr_opened,0)
 OR coalesce(p.closed,0)<>coalesce(s.pr_closed,0)
 OR coalesce(p.merged,0)<>coalesce(s.pr_merged,0);

SELECT repo_id FROM ads_repository_activity_trend
EXCEPT ALL SELECT repo_id FROM dws_repository_daily;

SELECT event_date,repo_id,pr_opened,pr_closed,pr_merged,issue_opened,issue_closed,coverage,observed_hours FROM ads_pr_issue_throughput
EXCEPT ALL SELECT event_date,repo_id,pr_opened,pr_closed,pr_merged,
issue_opened,issue_closed,coverage,observed_hours FROM dws_repository_daily;

SELECT coalesce(p.repo_id,s.repo_id) repo_id
FROM (SELECT repo_id,
 count(CASE WHEN action='opened' THEN 1 END) opened,
 count(CASE WHEN action='closed' THEN 1 END) closed
 FROM dwd_issue_event GROUP BY repo_id) p
FULL OUTER JOIN dws_repository_daily s ON p.repo_id=s.repo_id
WHERE coalesce(p.opened,0)<>coalesce(s.issue_opened,0)
 OR coalesce(p.closed,0)<>coalesce(s.issue_closed,0);

SELECT coalesce(p.repo_id,s.repo_id) repo_id
FROM (SELECT repo_id,count(*) n FROM dwd_release_event GROUP BY repo_id) p
FULL OUTER JOIN dws_repository_daily s ON p.repo_id=s.repo_id
WHERE coalesce(p.n,0)<>coalesce(s.release_events,0);

SELECT coalesce(p.repo_id,s.repo_id) repo_id
FROM (SELECT repo_id,
 count(CASE WHEN event_type='WatchEvent' THEN 1 END) watches,
 count(CASE WHEN event_type='ForkEvent' THEN 1 END) forks
 FROM dwd_event GROUP BY repo_id) p
FULL OUTER JOIN dws_repository_daily s ON p.repo_id=s.repo_id
WHERE coalesce(p.watches,0)<>coalesce(s.watch_events,0)
 OR coalesce(p.forks,0)<>coalesce(s.fork_events,0);

SELECT event_date,repo_id,event_count,push_events,active_actors,release_events,watch_events,fork_events,coverage,observed_hours FROM ads_repository_activity_trend
EXCEPT ALL SELECT event_date,repo_id,event_count,push_events,
active_actors,release_events,watch_events,fork_events,coverage,observed_hours FROM dws_repository_daily;

SELECT event_date,repo_id,event_count,push_events,
active_actors,release_events,watch_events,fork_events,coverage,observed_hours FROM dws_repository_daily
EXCEPT ALL SELECT event_date,repo_id,event_count,push_events,active_actors,release_events,watch_events,fork_events,coverage,observed_hours FROM ads_repository_activity_trend;

SELECT event_date,repo_id,pr_opened,pr_closed,pr_merged,
issue_opened,issue_closed,coverage,observed_hours FROM dws_repository_daily
EXCEPT ALL SELECT event_date,repo_id,pr_opened,pr_closed,pr_merged,issue_opened,issue_closed,coverage,observed_hours FROM ads_pr_issue_throughput;