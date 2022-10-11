{DEFAULT @limit = ''}
{DEFAULT @offset = ''}
{DEFAULT @order_by = ''}
{DEFAULT @ascending = 'ASC'}
{DEFAULT @required_benefit_sources = ''}
{DEFAULT @required_benefit_count = 0} -- Should always be the same as number of data sources

WITH benefit_t AS(
    SELECT TARGET_COHORT_ID, OUTCOME_COHORT_ID, COUNT(DISTINCT(SOURCE_ID)) AS THRESH_COUNT
    FROM @schema.result
    WHERE RR <= @benefit
        AND P_VALUE < @p_cut_value
        AND calibrated = @calibrated
        AND SOURCE_ID >= 0 -- NEGATIVE SOURCE IDS are reserved for meta analysis
    GROUP BY TARGET_COHORT_ID, OUTCOME_COHORT_ID
),

risk_t AS (
    SELECT TARGET_COHORT_ID, OUTCOME_COHORT_ID, COUNT(DISTINCT(SOURCE_ID)) AS THRESH_COUNT
    FROM @schema.result
    WHERE RR >= @risk
        AND P_VALUE < @p_cut_value
        AND calibrated = @calibrated
        AND SOURCE_ID >= 0 -- NEGATIVE SOURCE IDS are reserved for meta analysis
    GROUP BY TARGET_COHORT_ID, OUTCOME_COHORT_ID
)
{@required_benefit_sources != ''} ? {,
-- inner join to results that are from required data sources only
req_benefit_sources AS (
    SELECT TARGET_COHORT_ID, OUTCOME_COHORT_ID, COUNT(DISTINCT(SOURCE_ID)) AS required_count
    FROM @schema.result
    WHERE RR <= @benefit
        AND P_VALUE < @p_cut_value
        AND calibrated = @calibrated
        AND SOURCE_ID IN (@required_benefit_sources)
    GROUP BY TARGET_COHORT_ID, OUTCOME_COHORT_ID
)
}


SELECT
    fr.TARGET_COHORT_ID,
    t.COHORT_NAME as TARGET_COHORT_NAME,
    fr.OUTCOME_COHORT_ID,
    o.COHORT_NAME AS OUTCOME_COHORT_NAME,

    CASE
        WHEN risk_t.THRESH_COUNT IS NULL THEN 0
        ELSE risk_t.THRESH_COUNT
    END AS risk_count,

    CASE
        WHEN benefit_t.THRESH_COUNT IS NULL THEN 0
        ELSE benefit_t.THRESH_COUNT
    END AS benefit_count,
    mr2.I2 as I2,
    {@show_exposure_classes}?{STRING_AGG(distinct ec.EXPOSURE_CLASS_NAME, ';') as ECN,}
    ROUND(mr.RR, 2) as meta_RR
FROM @schema.result fr

    LEFT JOIN benefit_t ON benefit_t.TARGET_COHORT_ID = fr.TARGET_COHORT_ID AND benefit_t.OUTCOME_COHORT_ID = fr.OUTCOME_COHORT_ID
    LEFT JOIN risk_t ON risk_t.TARGET_COHORT_ID = fr.TARGET_COHORT_ID AND risk_t.OUTCOME_COHORT_ID = fr.OUTCOME_COHORT_ID

    {@required_benefit_sources != ''} ?{
    LEFT JOIN req_benefit_sources rbs ON rbs.TARGET_COHORT_ID = fr.TARGET_COHORT_ID AND rbs.OUTCOME_COHORT_ID = fr.OUTCOME_COHORT_ID
    }

    INNER JOIN @schema.target t ON t.target_cohort_id = fr.target_cohort_id
    INNER JOIN @schema.outcome o ON o.outcome_cohort_id = fr.outcome_cohort_id

    LEFT JOIN @schema.negative_control nc ON (
        nc.outcome_cohort_id = fr.outcome_cohort_id AND nc.target_cohort_id = fr.target_cohort_id
    )

    LEFT JOIN @schema.result mr ON (
        fr.outcome_cohort_id = mr.outcome_cohort_id AND
        fr.target_cohort_id = mr.target_cohort_id AND
        mr.calibrated = @calibrated AND
        mr.source_id = -99
    )
    LEFT JOIN @schema.result mr2 ON (
        fr.outcome_cohort_id = mr2.outcome_cohort_id AND
        fr.target_cohort_id = mr2.target_cohort_id AND
        mr2.calibrated = 0 AND -- BUG NO META ANALYSIS I2 fo calibrated data, but it should be the same
        mr2.source_id = -99
    )
    {@show_exposure_classes}?{
    INNER JOIN @schema.target_exposure_class tec ON tec.target_cohort_id = t.target_cohort_id
    INNER JOIN @schema.exposure_class ec ON ec.exposure_class_id = tec.exposure_class_id
    }
    WHERE fr.calibrated = @calibrated

    {@outcome_cohort_name_length} ? {AND o.COHORT_NAME IN (@outcome_cohort_names)}
    {@target_cohort_name_length} ? {AND t.COHORT_NAME IN (@target_cohort_names)}
    {@show_exposure_classes & @exposure_classes != ''} ? {AND ec.EXPOSURE_CLASS_NAME IN (@exposure_classes)}

       {@filter_by_meta_analysis} ? {
       AND mr.RR <= @benefit AND mr.P_VALUE < @p_cut_value
    } : {
    AND 1 = CASE
        WHEN benefit_t.THRESH_COUNT IS NULL AND @benefit_count = 0 THEN 1
        WHEN benefit_t.THRESH_COUNT >= @benefit_count THEN 1
        ELSE 0
    END
    }

    AND 1 = CASE
        WHEN risk_t.THRESH_COUNT IS NULL AND @risk_count = 0 THEN 1
        WHEN risk_t.THRESH_COUNT > @risk_count THEN 0
        ELSE 1
    END
    {@required_benefit_sources != ''} ? {AND rbs.required_count >= @required_benefit_count}
    {@filter_outcome_types} ? {AND o.type_id IN (@outcome_types)}
    GROUP BY fr.target_cohort_id, fr.outcome_cohort_id, t.COHORT_NAME, o.COHORT_NAME, risk_t.THRESH_COUNT, benefit_t.THRESH_COUNT, mr2.I2, mr.RR
    {@order_by != ''} ? {ORDER BY @order_by @ascending}
    {@limit != ''} ? {LIMIT @limit {@offset != ''} ? {OFFSET @offset} }
