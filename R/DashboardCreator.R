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


copyResults <- function(connection, targetConnection, config, dashboardConfig, resultDatabaseSchema, dbms) {
  ## 2. Copy relevant data in to dashboard schema
  copyTables <- list(
    ### --- Reference Tables ---###
    cohort_definition = list(
      subQuery = '@results_schema.cohort_definition'
    ),
    exposure_cohort = list(
      subQuery = "@results_schema.exposure_cohort WHERE cohort_definition_id IN (@cohort_definition_id)",
      params = list(cohort_definition_id = dashboardConfig$cohortIds)
    ),
    outcome_cohort = list(
      subQuery = "@results_schema.outcome_cohort WHERE cohort_definition_id IN (@cohort_definition_id)",
      params = list(cohort_definition_id = dashboardConfig$cohortIds)
    ),
    cohort_group_definition = list(
      subQuery = "@results_schema.cohort_group_definition"
    ),
    cohort_group = list(
      subQuery = "@results_schema.cohort_group"
    ),
    concept_set_definition = list(
      subQuery = "@results_schema.concept_set_definition"
    ),
    atlas_cohort_reference = list(
      subQuery = "@results_schema.atlas_cohort_reference"
    ),
    cohort_concept_set = list(
      subQuery = "@results_schema.cohort_concept_set"
    ),
    analysis_setting = list(
      subQuery = "@results_schema.analysis_setting WHERE analysis_id IN (@analysis_id)",
      params = list(analysis_id = dashboardConfig$analysisSettings)
    ),
    reference_version = list(
      subQuery = "@results_schema.reference_version"
    ),
    ### --- Results Tables ---###
    data_source = list(
      subQuery = "@results_schema.data_source WHERE source_id IN (@source_ids)",
      params = list(source_ids = dashboardConfig$dataSources)
    ),
    scc_result = list(
      subQuery = "@results_schema.scc_result WHERE
        {@is_exposure} ? {target_cohort_id} : {outcome_cohort_id} IN (@cohort_definition_id)
        AND analysis_id IN (@analysis_id)
        AND source_id IN (@source_ids)",
      params = list(cohort_definition_id = dashboardConfig$cohortIds,
                    analysis_id = dashboardConfig$analysisSettings,
                    source_ids = dashboardConfig$dataSources,
                    is_exposure = dashboardConfig$exposureDashboard)
    ),
    scc_stat = list(
      subQuery = "@results_schema.scc_stat WHERE
        {@is_exposure} ? {target_cohort_id} : {outcome_cohort_id} IN (@cohort_definition_id)
        AND analysis_id IN (@analysis_id)
        AND source_id IN (@source_ids)",
      params = list(cohort_definition_id = dashboardConfig$cohortIds,
                    analysis_id = dashboardConfig$analysisSettings,
                    source_ids = dashboardConfig$dataSources,
                    is_exposure = dashboardConfig$exposureDashboard)
    )
  )

  if (dbms == "sqlite") {
    copyDt <- FALSE
    ## use copy if in existing database
    copySql <- "SELECT * FROM"
    queryFunc <- DatabaseConnector::renderTranslateQuerySql
  } else {
    ## Import data if in separate database
    copySql <- "INSERT INTO @target_schema.@target_table SELECT * FROM"
    queryFunc <- DatabaseConnector::renderTranslateExecuteSql
    copyDt <- TRUE
  }

  for (table in names(copyTables)) {
    message("Copying table ", table)
    tParams <- copyTables[[table]]
    params <- list(
      sql = paste(copySql, tParams$subQuery),
      results_schema = config$resultsSchema,
      connection = connection
    )
    if (copyDt) {
      params$target_schema = resultDatabaseSchema
      params$target_table = table
    }
    result <- do.call(queryFunc, c(params, tParams$params))

    if (!copyDt) {
      DatabaseConnector::insertTable(
        connection = targetConnection,
        data = result,
        databaseSchema = resultDatabaseSchema,
        tableName = table,
        createTable = FALSE,
        useMppBulkLoad = TRUE,
        dropTableIfExists = FALSE
      )
    }
  }
}

#' Create stand-alone dashboaard database reward data
#' @details
#' Creates and sqlite database a set of configuration parameters that can be used with the Reward
#' Shiny application.
#'
#' @param configPath            Path to global configuration path
#' @param dashboardConfig       Dashboard configuration options (see create dashboard configuration file)
createDashboardDatabase <- function(configPath,
                                    dashboardConfigPath,
                                    sqliteDbFile = NULL,
                                    resultDatabaseSchema = NULL,
                                    cemConnectionDetails = list(),
                                    overwrite = FALSE) {

  stopifnot("Must specify either database schema or sqlite file " = !is.null(sqliteDbFile) | !is.null(resultDatabaseSchema))
  checkmate::assertFileExists(configPath)
  checkmate::assertFileExists(dashboardConfigPath)
  config <- loadGlobalConfiguration(configPath)
  connection <- DatabaseConnector::connect(config$connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  dashboardConfig <- loadDashboardConfiguration(dashboardConfigPath)

  if (!is.null(sqliteDbFile)) {
    message("Using sqlite file", sqliteDbFile)
    if (file.exists(sqliteDbFile) & !overwrite) {
      stop("sqlite file exits, set overwrite to TRUE to continue")
    }
    dbms <- "sqlite"
    resultDatabaseSchema <- "main"
    targetConnectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "sqlite",
                                                                          server = sqliteDbFile)
    targetConnection <- DatabaseConnector::connect(targetConnectionDetails)
    on.exit(DatabaseConnector::disconnect(targetConnection))
  } else {
    message("Creating database schema ", resultDatabaseSchema)
    # TODO: check if schema exists before running drop
    targetConnection <- connection
    targetConnectionDetails <- config$connectionDetails
    dbms <- config$connectionDetails$dbms

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

  # 2. Copyy results
  copyResults(connection, targetConnection, config, dashboardConfig, resultDatabaseSchema, dbms)


  ## 3. Add negative control outcomes for exposures within study
  model <- DashboardDataModel$new(dashboardConfigPath,
                                  connectionDetails = targetConnectionDetails,
                                  cemConnectionDetails = config$cemConnectionDetails,
                                  resultDatabaseSchema = resultDatabaseSchema,
                                  usePooledConnection = FALSE)

  if (dashboardConfig$exposureDashboard) {
    controlConcepts <- model$getNegativeControlConditions() %>%
      dplyr::select(.data$cohortDefinitionId,
                    negativeControlConceptId = .data$conceptId,
                    negativeControlCohortId = .data$outcomeCohortId) %>%
      dplyr::mutate(isOutcomeControl = 1)
  } else {
    controlConcepts <- model$getNegativeControlExposures() %>%
      dplyr::select(.data$cohortDefinitionId,
                    negativeControlConceptId = .data$conceptId,
                    negativeControlCohortId = .data$targetCohortId) %>%
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

  ## 4. Run Meta-analysis

  ## 5. Compute null distributions

  ## 6. Produce calibrated effect estimates

}
