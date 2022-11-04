
DROP TABLE IF EXISTS @schema.scc_result_load_table;
create table @schema.scc_result_load_table (
    source_id INT NOT NULL,
    analysis_id INT NOT NULL,
    outcome_cohort_id BIGINT NOT NULL,
    target_cohort_id BIGINT NOT NULL,
    rr NUMERIC,
    se_log_rr NUMERIC,
    log_rr NUMERIC,
    c_pt NUMERIC,
    t_pt NUMERIC,
    t_at_risk NUMERIC,
    c_at_risk NUMERIC,
    t_cases NUMERIC,
    c_cases NUMERIC,
    lb_95 NUMERIC,
    ub_95 NUMERIC,
    p_value NUMERIC,
    I2 NUMERIC,
    num_exposures NUMERIC
);

DROP TABLE IF EXISTS @schema.scc_stat_load_table;
create TABLE @schema.scc_stat_load_table (
    source_id INT NOT NULL,
    analysis_id INT NOT NULL,
    outcome_cohort_id BIGINT NOT NULL,
    target_cohort_id BIGINT NOT NULL,
    stat_type varchar(100),
    mean NUMERIC,
    sd NUMERIC,
    minimum NUMERIC,
    p_10 NUMERIC,
    p_25 NUMERIC,
    median NUMERIC,
    p_75 NUMERIC,
    p_90 NUMERIC,
    maximum NUMERIC,
    total NUMERIC
);