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

CONST_META_FILE_NAME <- "reward-meta-info.json"

#' Save atlas cohort references.
#' @description
#' Creates a cohort definition set that can be used in other packages
#'
#' @param config        RewardConfig instance
#' @param connection    DatabaseConnector connection
#' @param path          Path to export data to
#' @export
saveAtlasCohortRefs <- function(config,
                                connection,
                                exportPath) {
  checkmate::assert_class(config, "RewardConfig")

  csql <- "
  SELECT
    acr.cohort_definition_id AS cohort_id,
    acr.atlas_id,
    acr.definition,
    acr.sql_definition,
    cd.short_name as cohort_name
  FROM @schema.atlas_cohort_reference acr
  INNER JOIN @schema.cohort_definition cd ON cd.cohort_definition_id = acr.cohort_definition_id
  LEFT JOIN @schema.cohort_subset_target ccs ON cd.cohort_definition_id = ccs.subset_cohort_definition_id
  WHERE ccs.subset_cohort_definition_id IS NULL
  "

  data <- DatabaseConnector::renderTranslateQuerySql(connection,
                                                     csql,
                                                     schema = config$resultsSchema,
                                                     snakeCaseToCamelCase = TRUE)

  # Create atlasCohortsDefinitions csv
  dir.create(file.path(exportPath, "cohorts"), showWarnings = FALSE)
  dir.create(file.path(exportPath, "sql"), showWarnings = FALSE)
  dir.create(file.path(exportPath, "subset_definitions"), showWarnings = FALSE)

  files <- c()
  for (i in 1:nrow(data)) {
    row <- data[i,]
    sqlDef <- rawToChar(base64enc::base64decode(row$sqlDefinition))
    jsonDef <- rawToChar(base64enc::base64decode(row$definition))
    sqlFileName <- file.path(exportPath, "sql", paste0(row$cohortId, ".sql"))
    jsonFileName <- file.path(exportPath, "cohorts", paste0(row$cohortId, ".json"))
    SqlRender::writeSql(sqlDef, sqlFileName)
    write(jsonDef, file = jsonFileName)
    files <- c(files, jsonFileName, sqlFileName)
  }

  cohortInfo <- data %>% dplyr::select(cohortId, atlasId, cohortName)
  cohortInfoFile <- file.path(exportPath, "atlas_cohorts.csv")


  colnames(cohortInfo) <- SqlRender::camelCaseToSnakeCase(colnames(cohortInfo))
  write.csv(cohortInfo, cohortInfoFile, row.names = FALSE, na = "")


  csql <- "
  SELECT * FROM @schema.cohort_subset_definition"

  subsetData <- DatabaseConnector::renderTranslateQuerySql(connection,
                                                           csql,
                                                           schema = config$resultsSchema,
                                                           snakeCaseToCamelCase = TRUE)
  for (i in 1:nrow(subsetData)) {
    row <- subsetData[i,]
    json <- rawToChar(base64enc::base64decode(row$json))
    jsonFileName <- file.path(exportPath, "subset_definitions", paste0(row$subsetDefinitionId, ".json"))
    write(json, file = jsonFileName)
    files <- c(files, jsonFileName)
  }

  files <- c(files, cohortInfoFile)
  return(files)
}


#' @title
#' Export Reference tables
#' @description
#' Takes created reference tables (cohort definitions) from central rewardb and exports them to a zipped csv file
#' @param config                            global reward config
#' @param exportPath                        path to export files to before zipping
#' @param exportZipPath                     resulting zip files
#' @export
exportReferenceTables <- function(config,
                                  connection = NULL,
                                  exportPath = config$export,
                                  exportZipFile = "reward-references.zip") {
  scipen <- getOption("scipen")
  options(scipen = 999)
  exportPath <- normalizePath(exportPath)
  if (!dir.exists(exportPath)) {
    dir.create(exportPath)
  }

  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails = config$connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  # Collect all files and make a hashn
  meta <- list()
  meta$hashList <- list()
  meta$tableNames <- config$referenceTables
  meta$atlasCohortHash <- list()
  exportFiles <- saveAtlasCohortRefs(config, connection, exportPath)

  for (file in exportFiles) {
    relativePath <- R.utils::getRelativePath(file, exportPath)
    meta$atlasCohortHash[[relativePath]] <- tools::md5sum(file)[[1]]
  }

  for (table in config$referenceTables) {
    data <- DatabaseConnector::renderTranslateQuerySql(connection,
                                                       "SELECT * FROM @schema.@table;",
                                                       schema = config$resultsSchema,
                                                       table = table)

    if (table == "atlas_cohort_reference") {
      data <- data %>% dplyr::select(-"SQL_DEFINITION", -"DEFINITION")
    }

    if (table == "cohort_subset_definition") {
      data <- data %>% dplyr::select(-"JSON")
    }

    file <- file.path(exportPath, paste0(table, ".csv"))
    suppressWarnings({ write.csv(data, file, na = "", row.names = FALSE, fileEncoding = "UTF-8") })
    meta$hashList[[basename(file)]] <- tools::md5sum(file)[[1]]
  }
  exportFiles <- c(exportFiles, file.path(exportPath, paste0(config$referenceTables, ".csv")))
  metaDataFilename <- file.path(exportPath, CONST_META_FILE_NAME)
  jsonlite::write_json(meta, metaDataFilename)
  unlink(exportZipFile)
  DatabaseConnector::createZipFile(exportZipFile, c(metaDataFilename, exportFiles), rootFolder = exportPath)

  ParallelLogger::logInfo(paste("Created export zipfile", exportZipFile))

  options(scipen = scipen)
}
