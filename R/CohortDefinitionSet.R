.decodeBase64 <- function(x) {
  rawToChar(base64enc::base64decode(x))
}

#' Get Mock Cohort definition set for subset of reward cohorts.
#' @description
#' Useful in other analysis steps with ohdsi packages
#'
#' @export
#' @param rewardConfig      Reward configuration
#' @param cohortIds         Cohort Identifiers
getMockCohortDefinitionSet <- function(rewardConfig, cohortIds) {
  connection <- DatabaseConnector::connect(rewardConfig$connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  sql <- "
  SELECT
    cd.cohort_definition_id as cohort_id,
    cd.cohort_definition_name as cohort_name,
    COALESCE(ar.sql_definition, '') as sql,
    COALESCE(ar.definition, '{}') as json,
    CASE WHEN ar.definition IS NULL THEN 0 ELSE 1 END as has_json
  FROM @schema.@cohort_definition cd
  LEFT JOIN @schema.@atlas_cohort_reference ar ON cd.cohort_definition_id = ar.cohort_definition_id
  WHERE cd.cohort_definition_id IN (@cohort_ids)
  "

  res <- DatabaseConnector::renderTranslateQuerySql(connection,
                                                    sql,
                                                    cohort_ids = cohortIds,
                                                    schema = rewardConfig$resultsSchema,
                                                    atlas_cohort_reference = rewardConfig$referenceTables$atlasCohortReference,
                                                    cohort_definition = rewardConfig$referenceTables$cohortDefinition,
                                                    snakeCaseToCamelCase = TRUE)


  res <- res %>%
    dplyr::mutate(json = ifelse(.data$hasJson == 1, .decodeBase64(.data$json), .data$json)) %>%
    dplyr::mutate(sql = ifelse(.data$hasJson == 1, .decodeBase64(.data$sql), "SELECT NULL")) %>%
    dplyr::select(-"hasJson")


  attr(res, "isMockCohortDefinitionSet") <- TRUE
  return(res)
}