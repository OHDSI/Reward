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

#' Register a CDM or return sourceId of existing cdm
registerCdm <- function(config, connection, cdmInfoFile) {
  cdmInfo <- RJSONIO::fromJSON(SqlRender::readSql(cdmInfoFile))


  getSourceInfo <- function() {
    sql <- "SELECT * FROM @schema.data_source ds WHERE ds.source_key = '@database';"
    DatabaseConnector::renderTranslateQuerySql(connection,
                                               sql,
                                               schema = config$resultsSchema,
                                               database = cdmInfo$database,
                                               snakeCaseToCamelCase = TRUE)
  }

  sourceInfo <- getSourceInfo()
  if (nrow(sourceInfo) == 0) {
    message("Inserting CDM with source key ", cdmInfo$database)
    sql <- "
    SELECT
        CASE
          WHEN MAX(source_id) IS NULL THEN 1
          ELSE max(source_id) + 1
        END
    AS source_id FROM @schema.data_source"
    sourceId <- DatabaseConnector::renderTranslateQuerySql(connection,
                                                           sql,
                                                           schema = config$resultsSchema,
                                                           snakeCaseToCamelCase = TRUE) %>% dplyr::pull()


    sql <- "INSERT INTO @schema.data_source
    (source_id, source_name, source_key, version_date)
    VALUES(@source_id, '@source_name', '@source_key', now());
    "
    DatabaseConnector::renderTranslateExecuteSql(connection,
                                                 sql,
                                                 schema = config$resultsSchema,
                                                 source_id = sourceId,
                                                 source_name = cdmInfo$name,
                                                 source_key = cdmInfo$database)
  }
  sourceInfo <- getSourceInfo()
  cdmInfo$changedSourceId <- cdmInfo$sourceId != sourceInfo$sourceId

  if (cdmInfo$changedSourceId) {
    warning("Source id in file is not the same as the created id, update cdm config to use source id: ",
            sourceInfo$sourceId)
  }

  cdmInfo$sourceId <- sourceInfo$sourceId
  return(cdmInfo)
}


#' Import Results from CDM
#' @description
#' Verify and import results exported from CDMs
#'
#' @param config            Reward configuration object
#' @param resultsZipPath    path to exported results
#' @param connection        DatabaseConnector connection to reward databases
#' @param cleanup           Remove extracted csv files after inserting results
#' @export
importResults <- function(config, resultsZipPath, connection = NULL, cleanup = TRUE) {
  if (cleanup) {
    on.exit(unlink(config$exportPath, recursive = TRUE, force = TRUE), add = TRUE)
  }
  # connect to database
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails = config$connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  }

  utils::unzip(zipfile = resultsZipPath, exdir = config$exportPath, overwrite = TRUE)
  cdmInfoFile <- file.path(config$exportPath, "cdmInfo.json")
  if (!file.exists(cdmInfoFile)) {
    stop("Required CDM info file not attached")
  }

  cdmInfo <- registerCdm(config, connection, cdmInfoFile)
  files <- file.path(config$exportPath, list.files(config$exportPath, pattern = "*.csv"))
  # Import tables using bulk upload
  for (file in files) {
    # Regexp match files to upload to different tables
    if (isTRUE(grep("scc-result", basename(file)) >= 1)) {
      tableName <- "scc_result"
    } else if (isTRUE(grep("time_at_risk", basename(file)) >= 1)) {
      tableName <- "scc_stat"
    } else {
      next # Not handled results
    }
    skip <- 1 # The first row is the column names
    maxRow <- 1e6
    ParallelLogger::logInfo("Inserting file ", file, "into table ", tableName)
    data <- vroom::vroom(file, ",", show_col_types = FALSE, n_max = maxRow)
    while (nrow(data) > 0) {
      if (cdmInfo$changedSourceId) {
        data$source_id <- cdmInfo$sourceId
      }

      DatabaseConnector::insertTable(connection,
                                     databaseSchema = config$resultsSchema,
                                     tableName = tableName,
                                     data = data,
                                     dropTableIfExists = FALSE,
                                     createTable = FALSE,
                                     tempTable = FALSE,
                                     bulkLoad = TRUE)
      skip <- skip + maxRow
      data <- vroom::vroom(file, ",", show_col_types = FALSE, skip = skip, n_max = maxRow, col_names = colnames(data))
    }
    if (cleanup) {
      unlink(file, recursive = TRUE, force = TRUE)
    }
  }
}
