{DEFAULT @schema = 'reward'}
{DEFAULT @cohort_definition = 'cohort_definition'}
{DEFAULT @exposure_cohort = 'exposure_cohort'}
{DEFAULT @outcome_cohort = 'outcome_cohort'}
{DEFAULT @cohort_group_definition = 'cohort_group_definition'}
{DEFAULT @cohort_group = 'cohort_group'}
{DEFAULT @concept_set_definition = 'concept_set_definition'}
{DEFAULT @atlas_cohort_reference = 'atlas_cohort_reference'}
{DEFAULT @cohort_concept_set = 'cohort_concept_set'}
{DEFAULT @analysis_setting = 'analysis_setting'}
{DEFAULT @include_constraints = ''}

create table @schema.scc_result (
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
    {@include_constraints != ''} ? {,
    PRIMARY KEY (source_id, analysis_id, outcome_cohort_id, target_cohort_id),

    CONSTRAINT out_cohort_def_fk
      FOREIGN KEY(outcome_cohort_id)
	    REFERENCES @cohort_definition(COHORT_DEFINITION_ID),

    CONSTRAINT exp_cohort_def_fk
      FOREIGN KEY(target_cohort_id)
	    REFERENCES @cohort_definition(COHORT_DEFINITION_ID),

	CONSTRAINT source_id_fk
      FOREIGN KEY(source_id)
	    REFERENCES data_source(source_id),

    CONSTRAINT analysis_id_fk
      FOREIGN KEY(analysis_id)
	    REFERENCES analysis_setting(analysis_id)

    }
);

create TABLE @schema.scc_stat (
    source_id INT NOT NULL,
    analysis_id INT NOT NULL,
    outcome_cohort_id BIGINT NOT NULL,
    target_cohort_id BIGINT NOT NULL,
    stat_type varchar(100),
    mean NUMERIC,
    sd NUMERIC,
    min NUMERIC,
    p10 NUMERIC,
    p25 NUMERIC,
    median NUMERIC,
    p75 NUMERIC,
    p90 NUMERIC,
    max NUMERIC
    {@include_constraints != ''} ? {,
    PRIMARY KEY (source_id, analysis_id, outcome_cohort_id, target_cohort_id),

    CONSTRAINT out_cohort_def_fk
      FOREIGN KEY(outcome_cohort_id)
	    REFERENCES @cohort_definition(COHORT_DEFINITION_ID),

    CONSTRAINT exp_cohort_def_fk
      FOREIGN KEY(target_cohort_id)
	    REFERENCES @cohort_definition(COHORT_DEFINITION_ID),

	CONSTRAINT source_id_fk
      FOREIGN KEY(source_id)
	    REFERENCES data_source(source_id),

    CONSTRAINT analysis_id_fk
      FOREIGN KEY(analysis_id)
	    REFERENCES analysis_setting(analysis_id)
    }
);

create TABLE @schema.data_source (
    source_id INT {@include_constraints != ''} ? {PRIMARY KEY},
    source_name varchar,
    source_key varchar,
    cdm_version varchar,
    db_id varchar,
    version_date date
);

create TABLE @schema.outcome_null_distribution (
    source_id INT NOT NULL,
    analysis_id INT NOT NULL,
    outcome_type INT NOT NULL,
    target_cohort_id BIGINT NOT NULL,
    ingredient_concept_id INT not null,
    null_dist_mean NUMERIC,
    null_dist_sd NUMERIC,
    absolute_error NUMERIC,
    n_controls NUMERIC
    {@include_constraints != ''} ? {,
    PRIMARY KEY (source_id, analysis_id, target_cohort_id, outcome_type),
    CONSTRAINT exp_cohort_def_fk
      FOREIGN KEY(target_cohort_id)
	    REFERENCES @cohort_definition(COHORT_DEFINITION_ID),

	CONSTRAINT source_id_fk
      FOREIGN KEY(source_id)
	    REFERENCES data_source(source_id),

    CONSTRAINT analysis_id_fk
      FOREIGN KEY(analysis_id)
	    REFERENCES analysis_setting(analysis_id)
    }
);


create TABLE @schema.exposure_null_distribution (
    source_id INT NOT NULL,
    analysis_id INT NOT NULL,
    outcome_cohort_id BIGINT NOT NULL,
    null_dist_mean NUMERIC,
    null_dist_sd NUMERIC,
    absolute_error NUMERIC,
    n_controls NUMERIC
    {@include_constraints != ''} ? {,
    PRIMARY KEY (source_id, analysis_id, outcome_cohort_id),
    CONSTRAINT out_cohort_def_fk
      FOREIGN KEY(outcome_cohort_id)
	    REFERENCES @cohort_definition(COHORT_DEFINITION_ID),

	CONSTRAINT source_id_fk
      FOREIGN KEY(source_id)
	    REFERENCES data_source(source_id),

    CONSTRAINT analysis_id_fk
      FOREIGN KEY(analysis_id)
	    REFERENCES @analysis_setting(analysis_id)
    }
);

{@include_constraints != ''} ? {
create index sccr_idx on @schema.scc_result(outcome_cohort_id, target_cohort_id);
create index idx_exp_null_dist on @schema.exposure_null_distribution(outcome_cohort_id);
create index idx_out_null_dist on @schema.outcome_null_distribution(ingredient_concept_id);
}