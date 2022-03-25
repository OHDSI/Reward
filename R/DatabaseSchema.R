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

#' @title
#' Build postgres schema
#' @description
#' Build the reward database schema for postgres instance from scratch
#' This will also require a CEM schema to be built which uses the OHDSI Common Evidence Model to generate the matrix
#' of known assocations for OMOP Standard Vocabulary terms. This is required for generating any stats that require negative controls
#' @param configFilePath                        path to global reward config
#'
#' @importFrom utils packageVersion
#' @import SqlRender
#' @export
createRewardSchema <- function(configFilePath,
                               settingsFilePath = system.file("settings", "defaultSccArgs.json", package = utils::packageName())) {
  config <- loadGlobalConfiguration(configFilePath)
  connection <- DatabaseConnector::connect(connectionDetails = config$connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  message("creating rewardb results and reference schema")
  sql <- SqlRender::loadRenderTranslateSql(file.path("create", "referenceSchema.sql"),
                                           packageName = "RewardExecutionPackage",
                                           dbms = connection@dbms,
                                           schema = config$resultsSchema,
                                           include_constraints = TRUE)
  DatabaseConnector::executeSql(connection, sql)


  sql <- SqlRender::loadRenderTranslateSql(file.path("create", "resultsSchema.sql"),
                                           packageName = utils::packageName(),
                                           dbms = connection@dbms,
                                           schema = config$resultsSchema,
                                           include_constraints = TRUE)
  DatabaseConnector::executeSql(connection, sql)



  message("creating bulk cohort references")
  sql <- SqlRender::loadRenderTranslateSql(file.path("create", "cohortReferences.sql"),
                                           packageName = utils::packageName(),
                                           dbms = connection@dbms,
                                           schema = config$resultsSchema,
                                           version_number = utils::packageVersion(utils::packageName()),
                                           vocabulary_schema = config$vocabularySchema)
  DatabaseConnector::executeSql(connection, sql)

  addAnalysisSettingsJson(connection, config, settingsFilePath = settingsFilePath)
}

addAnalysisSetting <- function(connection, config, name, typeId, description, options) {
  jsonStr <- jsonlite::toJSON(options)
  optionsEnc <- base64enc::base64encode(charToRaw(jsonStr))
  iSql <- "INSERT INTO @schema.analysis_setting (analysis_id, type_id, analysis_name, description, options)
  SELECT
      CASE
        WHEN max(analysis_id) IS NULL THEN 1
        ELSE max(analysis_id) + 1
      END as analysis_id,
      '@type_id' as type_id,
      '@name' as analysis_name,
      '@description' as description,
      '@options' as options
  FROM @schema.analysis_setting"
  DatabaseConnector::renderTranslateExecuteSql(connection,
                                               iSql,
                                               name = name,
                                               type_id = typeId,
                                               description = description,
                                               options = optionsEnc,
                                               schema = config$resultsSchema)
}

addAnalysisSettingsJson <- function(connection,
                                    config,
                                    settingsFilePath = system.file("settings", "defaultSccArgs.json", package = utils::packageName())) {
  for (settings in RJSONIO::fromJSON(settingsFilePath)) {
    addAnalysisSetting(connection = connection,
                       config = config,
                       name = settings$name,
                       typeId = settings$typeId,
                       description = settings$description,
                       options = settings$options)
  }
}

#' @title
#' Insert atlas cohort ref to postgres db
#' @description
#' Adds atlas cohort to db reference, from web api
#' Inserts name/id in to custom cohort table
#' Maps condition and drug concepts of interest, any desecdants or if they're excluded from the cohort
#' Concepts not from drug domain are ignored for exposures, concepts not from condition domain are ignored for outcomes
#' @param connection                        DatabaseConnector::connection
#' @param config                            rewardb global config
#' @param atlasId                           id to atlas cohort to pull down
#' @param exposure                          If exposure, cohort is treated as an exposure, drug domains are captured
#' @export
insertAtlasCohortRef <- function(connection,
                                 config,
                                 atlasId,
                                 webApiUrl = NULL,
                                 cohortDefinition = NULL,
                                 sqlDefinition = NULL,
                                 exposure = FALSE) {
  if (is.null(webApiUrl)) {
    webApiUrl <- config$webApiUrl
  }

  referenceTable <- config$referenceTables$atlasCohortReference

  # Null is mainly used for test purposes
  if (is.null(cohortDefinition)) {
    ParallelLogger::logInfo(paste("pulling", atlasId))
    cohortDefinition <- ROhdsiWebApi::getCohortDefinition(atlasId, webApiUrl)
  }

  if (is.null(sqlDefinition)) {
    sqlDefinition <- ROhdsiWebApi::getCohortSql(cohortDefinition, webApiUrl, generateStats = FALSE)
  }

  encodedFormDefinition <- base64enc::base64encode(charToRaw(RJSONIO::toJSON(cohortDefinition)))
  encodedFormSql <- base64enc::base64encode(charToRaw(sqlDefinition))

  ParallelLogger::logInfo(paste("Checking if cohort already exists", atlasId))
  existingDt <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    "SELECT atlas_id, atlas_url FROM @schema.@reference_table
        WHERE definition = '@encoded_definition';",
    schema = config$resultsSchema,
    encoded_definition = encodedFormDefinition,
    reference_table = referenceTable
  )

  if (nrow(existingDt) != 0) {
    stop(paste("Cohort", atlasId, "already in database, use removeAtlasCohort to clear entry references"))
  }
  ParallelLogger::logInfo(paste("inserting", atlasId, webApiUrl))

  # Note that this is because there isn't a generic implemenation for autoincrementing keys
  sql <- "SELECT CASE WHEN max(cohort_definition_id) IS NULL THEN 1 ELSE max(cohort_definition_id) + 1 END
                    as ID FROM @schema.@reference_table"
  newEntry <- DatabaseConnector::renderTranslateQuerySql(connection,
                                                         sql,
                                                         schema = config$resultsSchema,
                                                         reference_table = referenceTable)
  cohortDefinitionId <- newEntry$ID[[1]]

  # Create reference and Get last insert as referent ID from sequence
  insertSql <- "INSERT INTO @schema.@reference_table
                    (cohort_definition_id, atlas_id, atlas_url, definition, sql_definition)
                      values (@cohort_defiinition_id, @atlas_id, '@atlas_url', '@definition', '@sql_definition')"
  # Insert and get newly generated id
  DatabaseConnector::renderTranslateExecuteSql(
    connection,
    sql = insertSql,
    schema = config$resultsSchema,
    cohort_defiinition_id = cohortDefinitionId,
    atlas_id = atlasId,
    atlas_url = gsub("'", "''", webApiUrl),
    definition = encodedFormDefinition,
    sql_definition = encodedFormSql,
    reference_table = referenceTable
  )

  DatabaseConnector::renderTranslateExecuteSql(
    connection,
    sql = "INSERT INTO @schema.cohort_definition
                            (cohort_definition_id, cohort_definition_name, short_name, concept_set_id)
                                     values (@cohort_definition_id, '@name', '@name', 99999999)",
    schema = config$resultsSchema,
    cohort_definition_id = cohortDefinitionId,
    name = gsub("'", "''", cohortDefinition$name)
  )

  if (exposure) {
    DatabaseConnector::renderTranslateExecuteSql(connection,
                                                 sql = "INSERT INTO @schema.exposure_cohort (cohort_definition_id, atc_flg) values (@cohort_definition_id, -1)",
                                                 schema = config$resultsSchema,
                                                 cohort_definition_id = cohortDefinitionId)
  } else {
    DatabaseConnector::renderTranslateExecuteSql(connection,
                                                 sql = "INSERT INTO @schema.outcome_cohort (cohort_definition_id, outcome_type) values (@cohort_definition_id, 3)",
                                                 schema = config$resultsSchema,
                                                 cohort_definition_id = cohortDefinitionId)
  }

  ParallelLogger::logInfo(paste("inserting concept reference", atlasId, webApiUrl))
  results <- data.frame()
  for (conceptSet in cohortDefinition$expression$ConceptSets) {
    for (item in conceptSet$expression$items) {
      if ((!exposure & item$concept$DOMAIN_ID == "Condition") | (exposure & item$concept$DOMAIN_ID == "Drug")) {
        isExcluded <- if (is.null(item$isExcluded)) 0 else as.integer(item$isExcluded)
        includeDescendants <- if (is.null(item$includeDescendants)) 0 else as.integer(item$includeDescendants)
        includeMapped <- if (is.null(item$includeMapped)) 0 else as.integer(item$includeMapped)
        results <- rbind(results, data.frame(
          COHORT_DEFINITION_ID = cohortDefinitionId,
          CONCEPT_ID = item$concept$CONCEPT_ID,
          IS_EXCLUDED = isExcluded,
          INCLUDE_MAPPED = includeMapped,
          include_descendants = includeDescendants
        )
        )
      }
    }
  }
  if (length(results)) {
    tableName <- paste(config$resultsSchema, config$referenceTables$cohortConceptSet, sep = ".")
    DatabaseConnector::dbAppendTable(connection, tableName, results)
  } else {
    warning("No Condition (outcome cohort) or Drug (exposure) domain references, automated negative control selection will fail for this cohort")
  }
}