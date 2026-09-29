"""Observed repository names only. Input: repo_id, repo_name, observed_at timestamp.
Rebuild over the complete selected snapshot, including late observations.
Caller MUST persist conflicts and reject publication before consuming dimension.
"""
def build(spark, observations):
    observations.select('repo_id','repo_name','observed_at').distinct().createOrReplaceTempView('m3_observations')
    conflicts=spark.sql('''SELECT repo_id, observed_at, sort_array(collect_set(repo_name)) AS observed_names
        FROM m3_observations GROUP BY repo_id, observed_at HAVING count(*) > 1''')
    conflicts.createOrReplaceTempView('m3_conflict_keys')
    dimension=spark.sql('''WITH safe AS (
        SELECT o.* FROM m3_observations o LEFT ANTI JOIN m3_conflict_keys c ON o.repo_id=c.repo_id
    ), ordered AS (
        SELECT *, lag(repo_name) OVER (PARTITION BY repo_id ORDER BY observed_at) AS previous_name FROM safe
    ), changes AS (
        SELECT repo_id, repo_name, split(repo_name, '/')[0] AS owner_name, observed_at AS valid_from
        FROM ordered WHERE previous_name IS NULL OR previous_name <> repo_name
    ), intervals AS (
        SELECT *, lead(valid_from) OVER (PARTITION BY repo_id ORDER BY valid_from) AS valid_to,
        sha2(to_json(named_struct('repo_name',repo_name,'owner_name',owner_name)),256) AS attribute_hash FROM changes
    ) SELECT sha2(to_json(named_struct('repo_id',repo_id,'valid_from',
        date_format(valid_from,"yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"),'attribute_hash',attribute_hash)),256) AS repo_sk,
        repo_id,repo_name,owner_name,valid_from,valid_to,valid_to IS NULL AS is_current,attribute_hash FROM intervals''')
    return dimension, conflicts

def as_of(spark, facts, dimension):
    facts.createOrReplaceTempView('m3_facts')
    dimension.createOrReplaceTempView('m3_dimension')
    return spark.sql('''SELECT f.*, d.repo_sk FROM m3_facts f LEFT JOIN m3_dimension d
        ON f.repo_id=d.repo_id AND f.observed_at>=d.valid_from
        AND (d.valid_to IS NULL OR f.observed_at<d.valid_to)''')

def require_publishable(conflicts, quarantine_path):
    """Retain conflicting keys in immutable Parquet before refusing publication."""
    if conflicts.limit(1).count():
        conflicts.write.mode('errorifexists').parquet(quarantine_path)
        raise ValueError('SCD2 simultaneous name conflict; publication blocked')
