--- Post processing tables for fast loading of stats
{DEFAULT @scc_target_source_counts = scc_target_source_counts}
{DEFAULT @scc_outcome_source_counts = scc_target_source_counts}

drop table if exists @result_schema.@scc_target_source_counts;
create table @result_schema.@scc_target_source_counts
as
SELECT target_cohort_id, analysis_id, source_id, count(*)
      FROM @result_schema.scc_result sccr
group by target_cohort_id, analysis_id, source_id;

drop table if exists @result_schema.@scc_outcome_source_counts;
create table @result_schema.@scc_outcome_source_counts
as
SELECT outcome_cohort_id, analysis_id, source_id, count(*)
      FROM @result_schema.scc_result sccr
group by outcome_cohort_id, analysis_id, source_id;