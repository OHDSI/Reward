SELECT DISTINCT
    t.cohort_definition_id as cohort_definition_id,
    c.concept_id as exposure_class_id
INTO @schema.cohort_exposure_class
FROM @vocabulary_schema.concept_ancestor ca
    INNER JOIN @vocabulary_schema.concept c on (ca.ancestor_concept_id = c.concept_id AND c.concept_class_id = 'ATC 3rd')
    INNER JOIN @schema.exposure_cohort t ON (t.referent_concept_id = ca.descendant_concept_id)

SELECT
    tec.exposure_class_id,
    c.concept_name as exposure_class_name
INTO
FROM @schema.cohort_exposure_class tec
INNER JOIN @vocabulary_schema.concept c ON tec.exposure_class_id = c.concept_id
;

