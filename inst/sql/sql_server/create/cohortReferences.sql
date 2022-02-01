/* OUTCOME DEFINITIONS */
-- Create outcome cohort definitions
create table #cpt_anc_grp as
select
  ca1.ancestor_concept_id
  , ca1.descendant_concept_id
from @vocabulary_schema.concept_ancestor ca1
inner join
(
  select
    c1.concept_id
    , c1.concept_name
    , c1.vocabulary_id
    , c1.domain_id
  from @vocabulary_schema.concept c1
  inner join @vocabulary_schema.concept_ancestor ca1
    on ca1.ancestor_concept_id = 441840 /* clinical finding */
    and c1.concept_id = ca1.descendant_concept_id
  where
  (
    ca1.min_levels_of_separation > 2
  	or c1.concept_id in (433736, 433595, 441408, 72404, 192671, 137977, 434621, 437312, 439847, 4171917, 438555, 4299449, 375258, 76784, 40483532, 4145627, 434157, 433778, 258449, 313878)
  )
  and c1.concept_name not like '%finding'
  and c1.concept_name not like 'disorder of%'
  and c1.concept_name not like 'finding of%'
  and c1.concept_name not like 'disease of%'
  and c1.concept_name not like 'injury of%'
  and c1.concept_name not like '%by site'
  and c1.concept_name not like '%by body site'
  and c1.concept_name not like '%by mechanism'
  and c1.concept_name not like '%of body region'
  and c1.concept_name not like '%of anatomical site'
  and c1.concept_name not like '%of specific body structure%'
  and c1.domain_id = 'Condition'
) t1
  on ca1.ancestor_concept_id = t1.concept_id
;

INSERT INTO @schema.cohort_concept_set (
    COHORT_DEFINITION_ID,
    CONCEPT_SET_ID,
    CONCEPT_ID,
    CONCEPT_NAME,
    IS_EXCLUDED,
    INCLUDE_DESCENDANTS,
    INCLUDE_MAPPED
)
select
  DISTINCT
  c1.concept_id * 1000 as COHORT_DEFINITION_ID,
  c1.concept_id as CONCEPT_SET_ID,
  c1.concept_id as CONCEPT_ID,
  c1.concept_name as CONCEPT_NAME,
  0,
  1,
  0
from #cpt_anc_grp ca1
inner join @vocabulary_schema.concept c1 on ca1.ancestor_concept_id = c1.concept_id;

INSERT INTO @schema.cohort_concept_set (
    COHORT_DEFINITION_ID,
    CONCEPT_SET_ID,
    CONCEPT_ID,
    CONCEPT_NAME,
    IS_EXCLUDED,
    INCLUDE_DESCENDANTS,
    INCLUDE_MAPPED
)
select
  DISTINCT
  c1.concept_id * 1000 + 1 as COHORT_DEFINITION_ID,
  c1.concept_id as CONCEPT_SET_ID,
  c1.concept_id as CONCEPT_ID,
  c1.concept_name,
  0,
  1,
  0
from
#cpt_anc_grp ca1
inner join @vocabulary_schema.concept c1
  on ca1.ancestor_concept_id = c1.concept_id;

INSERT INTO @schema.cohort_concept_set (
   COHORT_DEFINITION_ID,
    CONCEPT_SET_ID,
    CONCEPT_ID,
    CONCEPT_NAME,
    IS_EXCLUDED,
    INCLUDE_DESCENDANTS,
    INCLUDE_MAPPED
)
select
  DISTINCT
  c1.concept_id * 1000 + 2 as COHORT_DEFINITION_ID,
  c1.concept_id as CONCEPT_SET_ID,
  c1.concept_id as CONCEPT_ID,
  c1.concept_name,
  0,
  1,
  0
from
#cpt_anc_grp ca1
inner join @vocabulary_schema.concept c1
  on ca1.ancestor_concept_id = c1.concept_id;

--outcomes not requiring a hospitalization
INSERT INTO @schema.cohort_definition
( cohort_definition_id,
  cohort_definition_name
  ,	short_name
  , concept_set_id
)
select
  DISTINCT
  c1.concept_id * 1000,
  'Incident outcome of ' + c1.concept_name + ' - first occurence of diagnosis with 2 diagnosis codes ' as cohort_definition_name
  , 'Incident outcome of ' + c1.concept_name + '- TWO DX ' as short_name
  ,	c1.concept_id as CONCEPT_SET_ID
from
#cpt_anc_grp ca1
inner join @vocabulary_schema.concept c1
  on ca1.ancestor_concept_id = c1.concept_id
;

--outcomes not requiring a hospitalization
INSERT INTO @schema.outcome_cohort
( cohort_definition_id,
  referent_concept_id,
  outcome_type
)
select
  DISTINCT
  c1.concept_id * 1000,
  c1.concept_id,
  0 as outcome_type
from
#cpt_anc_grp ca1
inner join @vocabulary_schema.concept c1
  on ca1.ancestor_concept_id = c1.concept_id
;

--incident outcomes - first diagnosis, which eventually leads to hospitalization for same outcome
INSERT INTO @schema.cohort_definition
( cohort_definition_id,
  cohort_definition_name
  ,	short_name
  , CONCEPT_SET_ID
)
select
    DISTINCT
    c1.concept_id * 1000 + 1,
    'Incident outcome of ' + c1.concept_name + ' - first occurence of diagnosis which is observed in hospital in future ' as cohort_definition_name
  , 'Incident outcome of ' + c1.concept_name + '- WITH INP ' as short_name
  ,	c1.concept_id as CONCEPT_SET_ID
from
#cpt_anc_grp ca1
inner join @vocabulary_schema.concept c1
  on ca1.ancestor_concept_id = c1.concept_id
;

--outcomes not requiring a hospitalization
INSERT INTO @schema.outcome_cohort
( cohort_definition_id,
  referent_concept_id,
  outcome_type
)
select
  DISTINCT
  c1.concept_id * 1000 + 1,
  c1.concept_id,
  1 as outcome_type
from
#cpt_anc_grp ca1
inner join @vocabulary_schema.concept c1
  on ca1.ancestor_concept_id = c1.concept_id
;

--incident outcomes - requring 1 diagnosis code
INSERT INTO @schema.cohort_definition
(
  cohort_definition_id,
  cohort_definition_name
  ,	short_name
  , CONCEPT_SET_ID
)
select
    DISTINCT
    c1.concept_id * 1000 + 2,
    'Incident outcome of ' + c1.concept_name + ' - first occurence of diagnosis ' as cohort_definition_name
  , 'Incident outcome of ' + c1.concept_name + '- ONE DX ' as short_name
  ,	c1.concept_id as CONCEPTSET_ID
from
#cpt_anc_grp ca1
inner join @vocabulary_schema.concept c1
  on ca1.ancestor_concept_id = c1.concept_id
;

--outcomes not requiring a hospitalization
INSERT INTO @schema.outcome_cohort
( cohort_definition_id,
  referent_concept_id,
  outcome_type
)
select
  DISTINCT
  c1.concept_id * 1000 + 2,
  c1.concept_id,
  2 as outcome_type
from
#cpt_anc_grp ca1
inner join @vocabulary_schema.concept c1
  on ca1.ancestor_concept_id = c1.concept_id
;

truncate table #cpt_anc_grp;
drop table #cpt_anc_grp;

INSERT INTO @schema.cohort_definition (
    COHORT_DEFINITION_ID,
    COHORT_DEFINITION_NAME,
    SHORT_NAME,
    CONCEPT_SET_ID
)
SELECT DISTINCT
    CONCEPT_ID * 1000,
    CONCEPT_NAME,
    CONCAT(VOCABULARY_ID, ' - ', CONCEPT_NAME),
    CONCEPT_ID
FROM @vocabulary_schema.concept
WHERE (concept_class_id = 'Ingredient' AND vocabulary_id = 'RxNorm' AND standard_concept = 'S')
OR (concept_class_id = 'ATC 4th' AND vocabulary_id = 'ATC');

-- Create ingredient cohorts from vocabulary
INSERT INTO @schema.cohort_concept_set (
    COHORT_DEFINITION_ID,
    CONCEPT_SET_ID,
    CONCEPT_ID,
    CONCEPT_NAME,
    IS_EXCLUDED,
    INCLUDE_DESCENDANTS,
    INCLUDE_MAPPED
)
SELECT DISTINCT concept_id * 1000, concept_id, concept_id, concept_name, 0, 1, 0
    from @vocabulary_schema.concept
    where (concept_class_id = 'Ingredient' AND vocabulary_id = 'RxNorm' AND standard_concept = 'S')
       OR (concept_class_id = 'ATC 4th' AND vocabulary_id = 'ATC');

INSERT INTO @schema.exposure_cohort (
    cohort_definition_id,
    referent_concept_id,
    atc_flg
)

SELECT DISTINCT
    concept_id * 1000,
    concept_id,
    CASE WHEN vocabulary_id = 'ATC' THEN 1 else 0 END AS ATC_FLAG
    from @vocabulary_schema.concept
    where (concept_class_id = 'Ingredient' AND vocabulary_id = 'RxNorm' AND standard_concept = 'S')
       OR (concept_class_id = 'ATC 4th' AND vocabulary_id = 'ATC');