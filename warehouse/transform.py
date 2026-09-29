"""Deduplicate using narrow canonical positions; never sort all JSON payloads."""
def classify_duplicates(spark,ods):
    ods.createOrReplaceTempView("_ods_dedup_input")
    keys=spark.sql("""SELECT event_id,
        min(named_struct('source_hour',source_hour,'line_number',line_number)) canonical,
        count(*) copies FROM _ods_dedup_input GROUP BY event_id HAVING count(*)>1""").persist()
    count=keys.count()
    keys.createOrReplaceTempView("_duplicate_positions")
    strategy="BROADCAST" if count<=100000 else "SHUFFLE_HASH"
    classified=spark.sql(f"""SELECT /*+ {strategy}(d) */ o.*,
        d.canonical.source_hour canonical_source_hour,
        d.canonical.line_number canonical_line_number,
        (d.event_id IS NOT NULL AND
          (o.source_hour<>d.canonical.source_hour OR o.line_number<>d.canonical.line_number)) duplicate_row
        FROM _ods_dedup_input o LEFT JOIN _duplicate_positions d ON o.event_id=d.event_id""")
    classified.where("canonical_line_number is not null").select(*ods.columns).createOrReplaceTempView("_repeated_events")
    duplicate=classified.where("duplicate_row").drop("duplicate_row")
    unique=classified.where("NOT duplicate_row").drop("duplicate_row","canonical_source_hour","canonical_line_number")
    return unique,duplicate,{"duplicate_key_count":count,"strategy":strategy}
