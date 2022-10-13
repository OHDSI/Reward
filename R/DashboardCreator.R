# Copyright 2022 Observational Health Data Sciences and Informatics
#
# This file is part of Reward
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

copyResults <- function(connection, targetConnection, config, dashboardConfig, resultDatabaseSchema, copyData) {

  message("Creating subset of valid results for optimal dashboard queries")
  # Select subset of cohorts that actually exist
  cohortsUsedSql <- "CREATE TABLE #usable_cohorts AS (
     SELECT distinct target_cohort_id as cohort_definition_id FROM @schema.scc_result
     WHERE RR IS NOT NULL AND RR > 0.0
     {!@exposure_dash} ? {AND outcome_cohort_id IN (@cohort_ids)} : {AND target_cohort_id IN (@cohort_ids)}
     UNION
     SELECT distinct outcome_cohort_id FROM @schema.scc_result
     WHERE RR IS NOT NULL AND RR > 0.0
     {!@exposure_dash} ? {AND outcome_cohort_id IN (@cohort_ids)} : {AND target_cohort_id IN (@cohort_ids)}
  )"
  DatabaseConnector::renderTranslateExecuteSql(connection,
                                               cohortsUsedSql,
                                               schema = config$resultsSchema,
                                               exposure_dash = dashboardConfig$exposureDashboard,
                                               cohort_ids = dashboardConfig$cohortIds)

  ## Copy relevant data in to dashboard schema
  copyTables <- list(

    ### --- Reference Tables ---###
    cohort_definition = list(
      subQuery = 'SELECT cd.* FROM @results_schema.cohort_definition cd INNER JOIN #usable_cohorts uc ON uc.cohort_definition_id = cd.cohort_definition_id'
    ),
    exposure_cohort = list(
      subQuery = "SELECT cd.* FROM @results_schema.exposure_cohort cd
      INNER JOIN #usable_cohorts uc ON uc.cohort_definition_id = cd.cohort_definition_id
      {@is_exposure} ? {WHERE cd.cohort_definition_id IN (@cohort_definition_id)}",
      params = list(
        cohort_definition_id = dashboardConfig$cohortIds,
        is_exposure = dashboardConfig$exposureDashboard
      )
    ),
    outcome_cohort = list(
      subQuery = "SELECT cd.* FROM @results_schema.outcome_cohort cd
      INNER JOIN #usable_cohorts uc ON uc.cohort_definition_id = cd.cohort_definition_id
      {!@is_exposure} ? {WHERE cd.cohort_definition_id IN (@cohort_definition_id)}",
      params = list(
        cohort_definition_id = dashboardConfig$cohortIds,
        is_exposure = dashboardConfig$exposureDashboard
      )
    ),
    cohort_group_definition = list(
      subQuery = "SELECT * FROM @results_schema.cohort_group_definition"
    ),
    cohort_group = list(
      subQuery = "SELECT * FROM @results_schema.cohort_group"
    ),
    concept_set_definition = list(
      subQuery = "SELECT * FROM @results_schema.concept_set_definition"
    ),
    atlas_cohort_reference = list(
      subQuery = "SELECT cd.* FROM @results_schema.atlas_cohort_reference cd
      INNER JOIN #usable_cohorts uc ON uc.cohort_definition_id = cd.cohort_definition_id
      {@is_exposure} ? {WHERE cd.cohort_definition_id IN (@cohort_definition_id)}",
      params = list(
        cohort_definition_id = dashboardConfig$cohortIds,
        is_exposure = dashboardConfig$exposureDashboard
      )
    ),
    cohort_concept_set = list(
      subQuery = "SELECT cd.* FROM @results_schema.cohort_concept_set cd
      INNER JOIN #usable_cohorts uc ON uc.cohort_definition_id = cd.cohort_definition_id",
      params = list(
        cohort_definition_id = dashboardConfig$cohortIds
      )
    ),
    analysis_setting = list(
      subQuery = "SELECT * FROM @results_schema.analysis_setting WHERE analysis_id IN (@analysis_id)",
      params = list(analysis_id = dashboardConfig$analysisSettings)
    ),
    reference_version = list(
      subQuery = "SELECT * FROM  @results_schema.reference_version"
    ),
    ### --- Results Tables ---###
    data_source = list(
      subQuery = "SELECT * FROM @results_schema.data_source WHERE source_id IN (@source_ids)",
      params = list(source_ids = dashboardConfig$dataSources)
    ),

    scc_result = list(
      subQuery = "
  SELECT
    sr.*,
    0 as calibrated
     FROM  @results_schema.scc_result sr
        WHERE rr > 0.0 and rr is not null
        AND {@is_exposure} ? {target_cohort_id} : {outcome_cohort_id} IN (@cohort_definition_id)
        AND analysis_id IN (@analysis_id)
        AND source_id IN (@source_ids)",
      colnames = c("source_id", "analysis_id", "outcome_cohort_id", "target_cohort_id", "calibrated",
                   "rr", "se_log_rr", "log_rr", "c_pt", "t_pt", "t_at_risk", "c_at_risk", "t_cases", "c_cases",
                   "lb_95", "ub_95", "p_value", "I2", "num_exposures"),
      params = list(cohort_definition_id = dashboardConfig$cohortIds,
                    analysis_id = dashboardConfig$analysisSettings,
                    source_ids = dashboardConfig$dataSources,
                    is_exposure = dashboardConfig$exposureDashboard)
    ),
    scc_stat = list(
      subQuery = "SELECT * FROM  @results_schema.scc_stat WHERE
        {@is_exposure} ? {target_cohort_id} : {outcome_cohort_id} IN (@cohort_definition_id)
        AND analysis_id IN (@analysis_id)
        AND source_id IN (@source_ids)",
      params = list(cohort_definition_id = dashboardConfig$cohortIds,
                    analysis_id = dashboardConfig$analysisSettings,
                    source_ids = dashboardConfig$dataSources,
                    is_exposure = dashboardConfig$exposureDashboard)
    )
  )

  for (table in names(copyTables)) {
    message("Copying table ", table)
    tParams <- copyTables[[table]]
    params <- list(
      results_schema = config$resultsSchema,
      connection = connection
    )

    if (copyData | table == "scc_result") {
      params$sql <- tParams$subQuery
      result <- do.call(DatabaseConnector::renderTranslateQuerySql, c(params, tParams$params))
      DatabaseConnector::insertTable(
        connection = targetConnection,
        data = result,
        databaseSchema = resultDatabaseSchema,
        tableName = table,
        createTable = FALSE,
        bulkLoad = TRUE,
        dropTableIfExists = FALSE
      )

      # Repeat upload if meta-analysis is just for a single data source
      if (table == "scc_result" && length(dashboardConfig$dataSources) == 1) {
        result$SOURCE_ID <- -99
        DatabaseConnector::insertTable(
          connection = targetConnection,
          data = result,
          databaseSchema = resultDatabaseSchema,
          tableName = table,
          createTable = FALSE,
          bulkLoad = TRUE,
          dropTableIfExists = FALSE
        )
      }
    } else {
      params$sql <- paste0("INSERT INTO @target_schema.@target_table ", tParams$subQuery)
      params$target_schema <- resultDatabaseSchema
      params$target_table <- table
      do.call(DatabaseConnector::renderTranslateExecuteSql, c(params, tParams$params))
    }
  }
}

.computeNullDist <- function(negatives) {
  negatives <- negatives %>%
    dplyr::select(rr,
                  seLogRr) %>%
    tidyr::drop_na()

  if (nrow(negatives) == 0)
    return(data.frame())

  null <- EmpiricalCalibration::fitNull(logRr = log(negatives$rr), seLogRr = negatives$seLogRr)

  data.frame(
    mean = null['mean'],
    sd = null['sd'],
    absoluteError = EmpiricalCalibration::computeExpectedAbsoluteSystematicError(null),
    nControls = nrow(negatives)
  )
}

addNegativeControls <- function(model, targetConnection, resultDatabaseSchema, dashboardConfig) {
  message("Adding negative controls")
  if (dashboardConfig$exposureDashboard) {
    controlConcepts <- model$getNegativeControlConditions(dashboardConfig$cohortIds) %>%
      dplyr::select(cohortDefinitionId,
                    negativeControlConceptId = conceptId,
                    negativeControlCohortId = outcomeCohortId) %>%
      dplyr::mutate(isOutcomeControl = 1)
  } else {
    controlConcepts <- model$getNegativeControlExposures(dashboardConfig$cohortIds) %>%
      dplyr::select(cohortDefinitionId,
                    negativeControlConceptId = conceptId,
                    negativeControlCohortId = targetCohortId) %>%
      dplyr::mutate(isOutcomeControl = 0)
  }

  # Map concepts to outcomes/exposure cohorts
  DatabaseConnector::insertTable(
    connection = targetConnection,
    data = controlConcepts,
    databaseSchema = resultDatabaseSchema,
    tableName = "negative_control",
    createTable = FALSE,
    bulkLoad = TRUE,
    dropTableIfExists = FALSE,
    camelCaseToSnakeCase = TRUE
  )
}

.metaAnalysis <- function(table) {
  meta::settings.meta('meta4')
  table$i2 <- NA
  results <- meta::metainc(data = table,
                           event.e = tCases,
                           time.e = tPt,
                           event.c = cCases,
                           time.c = cPt,
                           sm = "IRR",
                           model.glmm = "UM.RS")

  # Return a single row with computed results
  row <- data.frame(sourceId = -99,
                    tAtRisk = sum(table$tAtRisk),
                    tPt = sum(table$tPt),
                    tCases = sum(table$tCases),
                    cAtRisk = sum(table$cAtRisk),
                    cPt = sum(table$cPt),
                    cCases = sum(table$cCases),
                    rr = exp(results$TE.random),
                    lb95 = exp(results$lower.random),
                    ub95 = exp(results$upper.random),
                    pValue = results$pval.random,
                    i2 = ifelse(is.na(results$I2), 0.0, results$I2))

  return(row)
}

computeMetaAnalysis <- function(targetConnection, resultDatabaseSchema) {
  message("Computing meta analysis, (may take some time)...")

  # NOTE - could apply batched operation for improved memory use
  fullResults <- DatabaseConnector::renderTranslateQuerySql(targetConnection,
                                                            "SELECT * FROM @schema.scc_result WHERE source_id != -99;",
                                                            schema = resultDatabaseSchema,
                                                            snakeCaseToCamelCase = TRUE)


  # For each distinct pair: (target, outcome) get all data sources
  # Run meta analysis
  # Write uncalibrated table
  # Calibrate meta analysis results
  results <- fullResults %>%
    dplyr::group_by(targetCohortId, outcomeCohortId, analysisId) %>%
    dplyr::group_modify(~.metaAnalysis(.x))

  colnames(results) <- SqlRender::camelCaseToSnakeCase(colnames(results))
  results <- results %>% dplyr::rename(I2 = i_2)
  DatabaseConnector::insertTable(
    connection = targetConnection,
    data = results,
    databaseSchema = resultDatabaseSchema,
    tableName = "scc_result",
    createTable = FALSE,
    bulkLoad = TRUE,
    dropTableIfExists = FALSE
  )
}

computeNullDistributions <- function(targetConnection, dashboardConfig, resultDatabaseSchema) {
  message("Computing null distributions")
  if (dashboardConfig$exposureDashboard) {
    sql <- "SELECT sccr.*, o.outcome_type
    FROM @schema.scc_result sccr
    INNER JOIN @schema.negative_control nc ON (sccr.target_cohort_id = nc.cohort_definition_id
        AND sccr.outcome_cohort_id = nc.negative_control_cohort_id)
    INNER JOIN @schema.outcome_cohort o ON sccr.outcome_cohort_id = o.cohort_definition_id
    WHERE o.outcome_type != 3
      "
    ## 5. Compute null distributions
    ncResults <- DatabaseConnector::renderTranslateQuerySql(targetConnection,
                                                            sql,
                                                            schema = resultDatabaseSchema,
                                                            snakeCaseToCamelCase = TRUE)

    nullDistResults <- ncResults %>%
      dplyr::group_by(targetCohortId, analysisId, sourceId, outcomeType) %>%
      dplyr::group_modify(~.computeNullDist(.x)) %>%
      dplyr::ungroup()

    # Apply outcome model 2 (1 diagnosis code) to ATLAS cohorts
    atlasDists <- nullDistResults %>%
      dplyr::filter(outcomeType == 2) %>%
      dplyr::mutate(outcomeType = 3)

    nullDistResults <- rbind(nullDistResults, atlasDists)

    DatabaseConnector::insertTable(
      connection = targetConnection,
      data = nullDistResults,
      databaseSchema = resultDatabaseSchema,
      tableName = "outcome_null_distribution",
      createTable = FALSE,
      bulkLoad = TRUE,
      dropTableIfExists = FALSE,
      camelCaseToSnakeCase = TRUE
    )

  } else {

    sql <- "SELECT sccr.*
    FROM @schema.scc_result sccr
    INNER JOIN @schema.negative_control nc ON sccr.outcome_cohort_id = nc.cohort_definition_id
      AND sccr.target_cohort_id = nc.negative_control_cohort_id
      "
    ## 5. Compute null distributions
    ncResults <- DatabaseConnector::renderTranslateQuerySql(targetConnection,
                                                            sql,
                                                            schema = resultDatabaseSchema,
                                                            snakeCaseToCamelCase = TRUE)

    nullDistResults <- ncResults %>%
      dplyr::group_by(outcomeCohortId, analysisId, sourceId) %>%
      dplyr::group_modify(~.computeNullDist(.x))

    DatabaseConnector::insertTable(
      connection = targetConnection,
      data = nullDistResults,
      databaseSchema = resultDatabaseSchema,
      tableName = "exposure_null_distribution",
      createTable = FALSE,
      bulkLoad = TRUE,
      dropTableIfExists = FALSE,
      camelCaseToSnakeCase = TRUE
    )
  }
}

calibrate <- function(x, nullDist) {
  errorModel <- EmpiricalCalibration::convertNullToErrorModel(nullDist)
  ci <- EmpiricalCalibration::calibrateConfidenceInterval(log(x$rr), x$seLogRr, errorModel)
  pvalue <- EmpiricalCalibration::calibrateP(nullDist, log(x$rr), x$seLogRr)

  result <- tibble::tibble(calibrated = 1,
                           tAtRisk = x$tAtRisk,
                           tPt = x$tPt,
                           tCases = x$tCases,
                           cAtRisk = x$cAtRisk,
                           cPt = x$cPt,
                           cCases = x$cCases,
                           pValue = pvalue,
                           ub95 = exp(ci$logUb95Rr),
                           lb95 = exp(ci$logLb95Rr),
                           rr = exp(ci$logRr),
                           logRr = ci$logRr,
                           seLogRr = ci$seLogRr,
                           i2 = x$i2)
  return(result)
}


.applyOutcomeCalibration <- function(x, nulls) {
  null <- nulls %>% dplyr::filter(sourceId == x %>% pull(sourceId) %>% unique(),
                                  targetCohortId == x %>% pull(targetCohortId) %>% unique(),
                                  outcomeType == x %>% pull(outcomeType) %>% unique(),
                                  analysisId == x %>% pull(analysisId) %>% unique())

  nullDist <- createNullDist(null$mean[1], null$sd[1])
  res <- calibrate(x, nullDist)
  res$outcomeCohortId <- x$outcomeCohortId
  return(res)
}

.applyExposureCalibration <- function(x, nulls) {
  null <- nulls %>% dplyr::filter(sourceId == x %>% pull(sourceId) %>% unique(),
                                  outcomeCohortId == x %>% pull(outcomeCohortId) %>% unique(),
                                  analysisId == x %>% pull(analysisId) %>% unique())

  nullDist <- createNullDist(null$mean[1], null$sd[1])
  res <- calibrate(x, nullDist)
  res$targetCohortId <- x$targetCohortId
  return(res)
}


computeCalibratedEstimates <- function(dashboardConfig, targetConnection, resultDatabaseSchema) {
  message("computing calibrated effect estimates for all exposure/outcome pairs")
  if (dashboardConfig$exposureDashboard) {
    sql <- "
  SELECT sccr.*, oc.outcome_type FROM @schema.scc_result sccr
    INNER JOIN @schema.outcome_cohort oc ON (
      oc.cohort_definition_id = sccr.outcome_cohort_id
    )
  "
    fullResults <- DatabaseConnector::renderTranslateQuerySql(targetConnection,
                                                              sql,
                                                              schema = resultDatabaseSchema,
                                                              snakeCaseToCamelCase = TRUE)

    nulls <- DatabaseConnector::renderTranslateQuerySql(targetConnection,
                                                        "SELECT * FROM @schema.outcome_null_distribution",
                                                        schema = resultDatabaseSchema,
                                                        snakeCaseToCamelCase = TRUE)

    # Apply empirical calibration as transformation to grouped entries
    calibratedEstimates <- fullResults %>%
      dplyr::group_by(sourceId,
                      targetCohortId,
                      outcomeType,
                      analysisId) %>%
      dplyr::group_modify(~.applyOutcomeCalibration(.x, nulls), .keep = TRUE) %>%
      dplyr::ungroup() %>%
      dplyr::select(-outcomeType)
  } else {
    sql <- "SELECT sccr.* FROM @schema.scc_result sccr "
    fullResults <- DatabaseConnector::renderTranslateQuerySql(targetConnection,
                                                              sql,
                                                              schema = resultDatabaseSchema,
                                                              snakeCaseToCamelCase = TRUE)

    nulls <- DatabaseConnector::renderTranslateQuerySql(targetConnection,
                                                        "SELECT * FROM @schema.exposure_null_distribution",
                                                        schema = resultDatabaseSchema,
                                                        snakeCaseToCamelCase = TRUE)

    # Apply empirical calibration as transformation to grouped entries
    calibratedEstimates <- fullResults %>%
      dplyr::group_by(sourceId,
                      outcomeCohortId,
                      analysisId) %>%
      dplyr::group_modify(~.applyExposureCalibration(.x, nulls), .keep = TRUE) %>%
      dplyr::ungroup()
  }
  colnames(calibratedEstimates) <- SqlRender::camelCaseToSnakeCase(colnames(calibratedEstimates))
  calibratedEstimates <- calibratedEstimates %>%
    dplyr::rename(I2 = i_2) %>%
    dplyr::distinct()

  DatabaseConnector::insertTable(
    connection = targetConnection,
    data = calibratedEstimates,
    databaseSchema = resultDatabaseSchema,
    tableName = "scc_result",
    createTable = FALSE,
    bulkLoad = TRUE,
    dropTableIfExists = FALSE
  )
}

#' Create stand-alone dashboaard database reward data
#' @details
#' Creates and sqlite database a set of configuration parameters that can be used with the Reward
#' Shiny application.
#'
#' @param configPath                Path to global configuration path
#' @param dashboardConfigPath       Dashboard configuration path for config options (see create dashboard configuration file)
#' @param targetConnectionDetails   If Exporting to external database, use these connection details
createDashboardDatabase <- function(configPath,
                                    dashboardConfigPath,
                                    targetConnectionDetails = NULL,
                                    resultDatabaseSchema = NULL,
                                    overwrite = FALSE) {

  stopifnot("Must specify either database schema or sqlite file " = !is.null(targetConnectionDetails) | !is.null(resultDatabaseSchema))
  checkmate::assertFileExists(configPath)
  checkmate::assertFileExists(dashboardConfigPath)
  config <- loadGlobalConfiguration(configPath)
  connection <- DatabaseConnector::connect(config$connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  dashboardConfig <- loadDashboardConfiguration(dashboardConfigPath)

  if (!is.null(targetConnectionDetails)) {
    message("Using sqlite file", targetConnectionDetails)
    dbms <- targetConnectionDetails$dbms
    if (dbms == "sqlite")
      resultDatabaseSchema <- "main"

    targetConnection <- DatabaseConnector::connect(targetConnectionDetails)
    on.exit(DatabaseConnector::disconnect(targetConnection))
    copyData <- TRUE
  } else {
    message("Creating database schema ", resultDatabaseSchema)
    targetConnection <- connection
    targetConnectionDetails <- config$connectionDetails
    dbms <- config$connectionDetails$dbms
    copyData <- FALSE
  }

  if (dbms != "sqlite") {
    # TODO: check if schema exists/overwrite before running drop
    sql <- "DROP SCHEMA IF EXISTS @results_schema CASCADE;
    CREATE SCHEMA @results_schema;"

    DatabaseConnector::renderTranslateExecuteSql(connection,
                                                 sql = sql,
                                                 results_schema = resultDatabaseSchema)
  }

  ## 1. Create schema/tables in dashboard db/schema
  message("creating reward reference tables")
  sql <- SqlRender::loadRenderTranslateSql(file.path("create", "referenceSchema.sql"),
                                           packageName = "RewardExecutionPackage",
                                           dbms = dbms,
                                           schema = resultDatabaseSchema,
                                           store_atlas_refs = TRUE,
                                           include_constraints = dbms != "sqlite")
  DatabaseConnector::executeSql(targetConnection, sql)

  message("creating reward result schema")
  sql <- SqlRender::loadRenderTranslateSql(file.path("create", "resultsSchema.sql"),
                                           packageName = utils::packageName(),
                                           dbms = dbms,
                                           schema = resultDatabaseSchema,
                                           add_calibrated_columns = TRUE,
                                           include_constraints = dbms != "sqlite")
  DatabaseConnector::executeSql(targetConnection, sql)

  insertSql <- "INSERT INTO @schema.data_source (source_id, source_name, source_key, db_id)
   VALUES (-99, 'Meta-analysis', 'meta', 'meta');
                                               "
  DatabaseConnector::renderTranslateExecuteSql(targetConnection,
                                               insertSql,
                                               schema = resultDatabaseSchema)

  # 2. Copyy results
  copyResults(connection, targetConnection, config, dashboardConfig, resultDatabaseSchema, copyData)
  # 3. Add negative control outcomes for exposures within study
  model <- DashboardDataModel$new(dashboardConfigPath,
                                  connectionDetails = targetConnectionDetails,
                                  cemConnectionDetails = config$cemConnectionDetails,
                                  resultDatabaseSchema = resultDatabaseSchema,
                                  usePooledConnection = FALSE)
  addNegativeControls(model, targetConnection, resultDatabaseSchema, dashboardConfig)
  ## 4. Meta-analysis
  if (length(config$dataSources) > 1) {
    computeMetaAnalysis(targetConnection, resultDatabaseSchema)
  }
  # 5. Add null distributions
  computeNullDistributions(targetConnection, dashboardConfig, resultDatabaseSchema)
  # 6. Compute calibrated effect estimates
  computeCalibratedEstimates(dashboardConfig, targetConnection, resultDatabaseSchema)
}
