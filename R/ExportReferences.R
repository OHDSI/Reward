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
saveAtlasCohortRefs <- function(config,
                                connection,
                                exportPath = config$exportPath) {
  checkmate::assert_class(config, "RewardConfig")

  csql <- "
  SELECT
    acr.cohort_definition_id AS cohort_id,
    acr.atlas_id,
    acr.definition,
    acr.sql_definition,
    cd.short_name as cohort_name,
  FROM @schema.atlas_cohort_reference acr
  INNER JOIN @schema.cohort_definition cd ON cd.cohort_definition_id = acr.cohort_definition_id"

  data <- DatabaseConnector::renderTranslateQuerySql(connection,
                                                     csql,
                                                     schema = config$resultsSchema,
                                                     snakeCaseToCamelCase = TRUE)
  # Create atlasCohortsDefinitions csv
  dir.create(file.path(exportPath, "cohorts"))
  dir.create(file.path(exportPath, "sql"))

  files <- c()
  for (i in 1:nrow(data)) {
    row <- data[i,]
    sqlDef <- rawToChar(base64enc::base64decode(row$sqlDefinition))
    jsonDef <- rawToChar(base64enc::base64decode(row$definition))
    sqlFileName <- file.path(exportPath, "cohorts", paste0(row$cohortId, ".sql"))
    jsonFileName <- file.path(exportPath, "sql", paste0(row$cohortId, ".json"))
    write(sqlDef, file = sqlFileName)
    write(jsonDef, file = jsonFileName)
    files <- c(files, jsonFileName, sqlFileName)
  }

  cohortInfo <- data %>% select(.data$cohortId, .data$atlasId, .data$cohortName)
  cohortInfoFile <- file.path(exportPath, "AtlasCohorts.csv")
  write.csv(cohortInfo, cohortInfoFile, row.names = FALSE, na = "")
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
                                  exportPath = tempdir(),
                                  exportZipFile = "reward-references.zip") {
  scipen <- getOption("scipen")
  options(scipen = 999)

  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails = config$connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  # Collect all files and make a hash
  meta <- list()
  meta$hashList <- list()
  meta$tableNames <- config$referenceTables
  meta$atlasCohortHash <- list()
  exportFiles <- saveAtlasCohortRefs(config, connection)

  for (file in exportFiles) {
    meta$atlasCohortHash[[file]] <- tools::md5sum(file)[[1]]
  }

  for (table in config$referenceTables) {
    data <- DatabaseConnector::renderTranslateQuerySql(connection,
                                                       "SELECT * FROM @schema.@table;",
                                                       schema = config$resultsSchema,
                                                       table = table)

    if (table == "atlas_cohort_reference") {
      data <- data %>% select(-.data$SQL_DEFINITION, -.data$DEFINITION)
    }

    file <- file.path(exportPath, paste0(table, ".csv"))
    suppressWarnings({ write.csv(data, file, na = "", row.names = FALSE, fileEncoding = "ascii") })
    meta$hashList[[basename(file)]] <- tools::md5sum(file)[[1]]
  }

  metaDataFilename <- file.path(exportPath, CONST_META_FILE_NAME)
  jsonlite::write_json(meta, metaDataFilename)

  exportFiles <- c(exportFiles, file.path(exportPath, paste0(config$referenceTables, ".csv")))
  zip::zipr(exportZipFile, append(exportFiles, metaDataFilename), include_directories = FALSE)

  ParallelLogger::logInfo(paste("Created export zipfile", exportZipFile))

  options(scipen = scipen)
}