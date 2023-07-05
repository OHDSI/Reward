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
#' @export
createRewardSchema <- function(configFilePath,
                               settingsFilePath = system.file("settings", "defaultSccArgs.json", package = utils::packageName())) {
  config <- loadGlobalConfiguration(configFilePath)
  connection <- DatabaseConnector::connect(connectionDetails = config$connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  message("creating reward results and reference schema")
  sql <- SqlRender::loadRenderTranslateSql(file.path("create", "referenceSchema.sql"),
                                           packageName = "RewardExecutionPackage",
                                           dbms = config$connectionDetails$dbms,
                                           schema = config$resultsSchema,
                                           store_atlas_refs = TRUE,
                                           include_constraints = config$connectionDetails$dbms != "sqlite")
  DatabaseConnector::executeSql(connection, sql)


  sql <- SqlRender::loadRenderTranslateSql(file.path("create", "resultsSchema.sql"),
                                           packageName = utils::packageName(),
                                           dbms = config$connectionDetails$dbms,
                                           schema = config$resultsSchema,
                                           include_constraints = config$connectionDetails$dbms != "sqlite")
  DatabaseConnector::executeSql(connection, sql)

  RewardExecutionPackage::migrateDatabaseModel(config, schema = config$resultsSchema)

  message("creating bulk cohort references")
  sql <- SqlRender::loadRenderTranslateSql(file.path("create", "cohortReferences.sql"),
                                           packageName = utils::packageName(),
                                           dbms = config$connectionDetails$dbms,
                                           schema = config$resultsSchema,
                                           version_number = utils::packageVersion(utils::packageName()),
                                           vocabulary_schema = config$vocabularySchema)
  DatabaseConnector::executeSql(connection, sql)

  addAnalysisSettingsJson(connection, config, settingsFilePath = settingsFilePath)
}


#' Migrate database model
#' @export
#' @inheritParams createRewardSchema
migrateDatabase <- function(configFilePath) {
  config <- loadGlobalConfiguration(configFilePath)
  RewardExecutionPackage::migrateDatabaseModel(config, schema = config$resultsSchema)
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
#' Insert atlas cohort definition set in to database
#' @description
#' Creates individual references in Reward db for each cohort definition in the cohort definition set.
#' Note: Call this function separatley for exposure and outcome cohorts and different atlas sources.
#'
#' @export
#' @param connection                        DatabaseConnector::connection
#' @param config                            rewardb global config
#' @param cohortDefinitionSet               A data.frame of cohorts containing -> cohortId, cohortName, json, sql fields
#' @param webApiUrl                         atlas source the definition set comes from (insert definitions from different sources sepratley)
#' @param exposure                          If exposure, cohort is treated as an exposure, drug domains are captured
addCohortDefinitionSet <- function(connection,
                                   config,
                                   cohortDefinitionSet,
                                   webApiUrl = NULL,
                                   exposure = FALSE) {

  checkmate::assertDataFrame(cohortDefinitionSet, min.rows = 1, col.names = "named")
  checkmate::assertNames(names(cohortDefinitionSet), must.include = c("cohortId", "cohortName", "sql", "json"))

  subsetDefs <- CohortGenerator::getSubsetDefinitions(cohortDefinitionSet)
  if (length(subsetDefs)) {
    subsetCohorts <- cohortDefinitionSet %>% dplyr::filter(.data$isSubset)
    cohortDefinitionSet <- cohortDefinitionSet %>% dplyr::filter(!.data$isSubset)
  }

  atlasToRewardMap <- data.frame()

  for (row in 1:nrow(cohortDefinitionSet)) {
    cohortDefinition <- list(
      name = cohortDefinitionSet[row,]$cohortName,
      id = cohortDefinitionSet[row,]$cohortId,
      expression = RJSONIO::fromJSON(cohortDefinitionSet[row,]$json)
    )
    tryCatch({
      createdId <- insertAtlasCohortRef(connection,
                                        config,
                                        cohortDefinitionSet[row,]$cohortId,
                                        webApiUrl = webApiUrl,
                                        cohortDefinition,
                                        cohortDefinitionSet[row,]$sql,
                                        exposure = exposure)

      atlasToRewardMap <- dplyr::bind_rows(atlasToRewardMap,
                                           data.frame(atlasId = cohortDefinitionSet[row,]$cohortId,
                                                      cohortDefinitionId = createdId))

    }, error = function(err) {
      print(paste("Error inserting Cohort ", cohortDefinitionSet[row,]$cohortId, "\n", err))
    })
  }

  for (subsetDef in subsetDefs) {
    targetCohortIds <- sapply(subsetDef$targetOutputPairs,
                              function(x) {
                                atlasToRewardMap[atlasToRewardMap$atlasId == x[1],]$cohortDefinitionId
                              })
    addSubsetDefinition(connection,
                        config,
                        subsetDef,
                        targetCohortIds,
                        exposure)
  }
}

.insertCustomCohort <- function(connection, config, atlasId, name, webApiUrl, encodedFormDefinition, encodedFormSql, exposure, isSubset = FALSE, subsetParent = NULL) {
  ParallelLogger::logInfo(paste("inserting", atlasId, webApiUrl))
  # Note that this is because there isn't a generic implemenation for autoincrementing keys
  sql <- "SELECT CASE
                    WHEN max(cd.cohort_definition_id) IS NULL THEN 1
                    ELSE max(cd.cohort_definition_id) + 1
                  END as ID
          FROM @schema.cohort_definition cd
          INNER JOIN @schema.atlas_cohort_reference acr ON acr.cohort_definition_id = cd.cohort_definition_id
          "
  newEntry <- DatabaseConnector::renderTranslateQuerySql(connection,
                                                         sql,
                                                         schema = config$resultsSchema)
  cohortDefinitionId <- newEntry$ID[[1]]

  if (!isSubset) {
    subsetParent <- cohortDefinitionId
  }

  DatabaseConnector::renderTranslateExecuteSql(
    connection,
    sql = "INSERT INTO @schema.cohort_definition
                            (cohort_definition_id, cohort_definition_name, short_name, concept_set_id, is_subset, subset_parent)
                                     values (@cohort_definition_id, '@name', '@name', 99999999, @is_subset, @subset_parent)",
    schema = config$resultsSchema,
    cohort_definition_id = cohortDefinitionId,
    subset_parent = subsetParent,
    is_subset = ifelse(isSubset, 1, 0),
    name = gsub("'", "''", name)
  )

  # Create reference and Get last insert as referent ID from sequence
  insertSql <- "INSERT INTO @schema.atlas_cohort_reference
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
    sql_definition = encodedFormSql
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

  return(cohortDefinitionId)
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
                                 cohortDefinition,
                                 sqlDefinition,
                                 exposure = FALSE) {
  if (is.null(webApiUrl)) {
    webApiUrl <- config$webApiUrl
  }

  referenceTable <- config$referenceTables$atlasCohortReference

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

  cohortDefinitionId <- .insertCustomCohort(connection, config, atlasId, cohortDefinition$name, webApiUrl, encodedFormDefinition, encodedFormSql, exposure)

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

  return(invisible(cohortDefinitionId))
}

#' Add subset definition
#' @description
#' Adds subset definition to reward reference database
#' @export
#' @param connection a
#' @param config b
#' @param cohortSubsetDefinition subset
#' @param targetCohortIds target cohorts
#' @param exposure TRUE
addSubsetDefinition <- function(connection,
                                config,
                                cohortSubsetDefinition,
                                targetCohortIds,
                                exposure = TRUE) {

  checkmate::assertR6(cohortSubsetDefinition, "CohortSubsetDefinition")

  sql <- "SELECT CASE
                    WHEN max(subset_definition_id) IS NULL THEN 1
                    ELSE max(subset_definition_id) + 1
                  END as ID
          FROM @schema.cohort_subset_definition"
  newEntry <- DatabaseConnector::renderTranslateQuerySql(connection,
                                                         sql,
                                                         schema = config$resultsSchema)

  # Always set stored Id to 0 for consistent check
  cohortSubsetDefinition$definitionId <- 0
  definitionId <- newEntry$ID[[1]]
  encodedDefinition <- base64enc::base64encode(charToRaw(cohortSubsetDefinition$toJSON()))

  ParallelLogger::logInfo(paste("Checking if cohort already exists"))
  existingDt <- DatabaseConnector::renderTranslateQuerySql(
    connection,
    "SELECT subset_definition_id FROM @schema.cohort_subset_definition
        WHERE json = '@encoded_definition';",
    schema = config$resultsSchema,
    encoded_definition = encodedDefinition
  )

  if (nrow(existingDt) != 0) {
    stop(paste("Subset", existingDt, "already in database"))
  }

  # Check targetCohortIds are all in db
  sql <- "SELECT cohort_definition_id, short_name FROM @reward_schema.cohort_definition
  WHERE cohort_definition_id in (@target_cohort_ids)"

  targetCohorts <- DatabaseConnector::renderTranslateQuerySql(connection,
                                                              sql,
                                                              reward_schema = config$resultsSchema,
                                                              target_cohort_ids = targetCohortIds,
                                                              snakeCaseToCamelCase = TRUE)

  checkmate::assertTRUE(all(targetCohortIds %in% targetCohorts$cohortDefinitionId))


  sql <- "
  INSERT INTO @reward_schema.cohort_subset_definition
      (subset_definition_id, subset_name, json)
      VALUES (@subset_definition_id, '@subset_name', '@json');
  "
  # Insert definition
  DatabaseConnector::renderTranslateExecuteSql(connection,
                                               sql,
                                               subset_definition_id = definitionId,
                                               reward_schema = config$resultsSchema,
                                               subset_name = cohortSubsetDefinition$name,
                                               json = encodedDefinition)

  for (target in targetCohortIds) {
    targetName <- targetCohorts %>%
      dplyr::filter(.data$cohortDefinitionId == target) %>%
      dplyr::select("shortName") %>%
      dplyr::pull()

    name <- paste(targetName, " - ", cohortSubsetDefinition$name)

    subsetCohortId <- .insertCustomCohort(connection,
                                          config,
                                          target,
                                          name,
                                          paste("SUBSET DEF - ", definitionId),
                                          base64enc::base64encode(charToRaw("{}")),
                                          base64enc::base64encode(charToRaw("SELECT NULL")),
                                          exposure,
                                          isSubset = TRUE,
                                          subsetParent = target)

    sql <- "INSERT INTO @reward_schema.cohort_subset_target
      (subset_definition_id, cohort_definition_id, subset_cohort_definition_id)
      VALUES (@subset_definition_id, @cohort_definition_id, @subset_cohort_definition_id);"
    DatabaseConnector::renderTranslateExecuteSql(connection,
                                                 sql,
                                                 reward_schema = config$resultsSchema,
                                                 subset_definition_id = definitionId,
                                                 subset_cohort_definition_id = subsetCohortId,
                                                 cohort_definition_id = target)

    sql <- "
    INSERT INTO @reward_schema.cohort_concept_set
    SELECT
      @subset_cohort_id as cohort_definition_id,
      concept_set_id,
      concept_id bigint,
      concept_name,
      is_excluded,
      include_descendants,
      include_mapped
	FROM  @reward_schema.cohort_concept_set
    WHERE cohort_definition_id = @target_cohort_id"
    DatabaseConnector::renderTranslateExecuteSql(connection,
                                                 sql,
                                                 reward_schema = config$resultsSchema,
                                                 target_cohort_id = target,
                                                 subset_cohort_id = subsetCohortId)

  }

}
