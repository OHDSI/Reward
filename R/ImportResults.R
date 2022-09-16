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
#' @description
#' This function adds a CDM to reward - however, the config file is not fully required.
#' Only the meta-data fields `database` and `name` are required.
#'
#' When a cdm is registered the database field is a unique idenitifier.
#'
#' @param config                Reward config
#' @param connection            connection to Reward db instance
#' @param cdmInfoFile           path to configruations
#' @export
registerCdm <- function(config, connection, cdmInfoFile) {
  cdmInfo <- RJSONIO::fromJSON(SqlRender::readSql(cdmInfoFile))
  getSourceInfo <- function() {
    sql <- "SELECT source_id, source_name, source_key FROM @schema.data_source ds WHERE ds.source_key = '@database';"
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
    VALUES(@source_id, '@source_name', '@source_key', '@dt');
    "
    DatabaseConnector::renderTranslateExecuteSql(connection,
                                                 sql,
                                                 schema = config$resultsSchema,
                                                 source_id = sourceId,
                                                 source_name = cdmInfo$name,
                                                 source_key = cdmInfo$database,
                                                 dt = Sys.time())
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


computeSourceCounts <- function(config, connection) {
  ParallelLogger::logInfo("(Re)creating source count tables")
  sql <- SqlRender::loadRenderTranslateSql(file.path("post_process", "source_counts.sql"),
                                           result_schema = config$resultsSchema,
                                           packageName = utils::packageName(),
                                           dbms = config$connectionDetails$dbms)

  DatabaseConnector::executeSql(connection, sql)
}


#' Import Results from CDM
#' @description
#' Verify and import results exported from CDMs
#'
#' @param config                        Reward configuration object
#' @param resultsZipPath                path to exported results
#' @param connection                    DatabaseConnector connection to reward databases
#' @param cleanup                       Remove extracted csv files after inserting results
#' @param computeAggregateTables        compute tables that count aggregated stats across different data sources
#' @export
importResults <- function(config,
                          resultsZipPath,
                          connection = NULL,
                          cleanup = TRUE,
                          computeStatsTables = TRUE) {
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
  uploadChunk <- function(chunk, pos) {
    if (cdmInfo$changedSourceId) {
      chunk$source_id <- cdmInfo$sourceId
    }
    DatabaseConnector::insertTable(connection,
                                   databaseSchema = config$resultsSchema,
                                   tableName = tableName,
                                   data = chunk,
                                   dropTableIfExists = FALSE,
                                   createTable = FALSE,
                                   tempTable = FALSE,
                                   bulkLoad = TRUE)
  }

  for (file in files) {
    # Regexp match files to upload to different tables
    if (isTRUE(grep("scc-result", basename(file)) >= 1)) {
      tableName <- "scc_result"
    } else if (isTRUE(grep("time_at_risk", basename(file)) >= 1)) {
      tableName <- "scc_stat"
    } else {
      next # Not handled results
    }
    ParallelLogger::logInfo("Inserting file ", file, "into table ", tableName)
    readr::read_csv_chunked(
      file = file,
      callback = uploadChunk,
      chunk_size = 1e7,
      guess_max = 1e6,
      progress = FALSE
    )

    if (cleanup) {
      unlink(file, recursive = TRUE, force = TRUE)
    }
  }

  if (computeStatsTables) {
    computeSourceCounts(config, connection)
  }
}

