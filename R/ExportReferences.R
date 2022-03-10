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

  for (table in config$referenceTables) {
    data <- DatabaseConnector::renderTranslateQuerySql(connection,
                                                       "SELECT * FROM @schema.@table;",
                                                       schema = config$resultsSchema,
                                                       table = table)

    file <- file.path(exportPath, paste0(table, ".csv"))
    suppressWarnings({ write.csv(data, file, na = "", row.names = FALSE, fileEncoding = "ascii") })
    meta$hashList[[basename(file)]] <- tools::md5sum(file)[[1]]
  }

  metaDataFilename <- file.path(exportPath, CONST_META_FILE_NAME)
  jsonlite::write_json(meta, metaDataFilename)

  exportFiles <- file.path(exportPath, paste0(config$referenceTables, ".csv"))
  zip::zipr(exportZipFile, append(exportFiles, metaDataFilename), include_directories = FALSE)

  ParallelLogger::logInfo(paste("Created export zipfile", exportZipFile))

  options(scipen = scipen)
}