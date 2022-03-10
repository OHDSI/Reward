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
  # Verify results
  files <- unzipAndVerifyResultsZip(resultsZipPath, config$exportPath)
  # connect to database
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails = config$connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  }

  # Import tables using bulk upload
  for (file in files) {
    if (isTRUE(grep("scc-result", basename(file)) >= 1)) {
      tableName <- "scc_result"
       data <- vroom::vroom(file, ",", show_col_types = FALSE)
    } else if (isTRUE(grep("time", basename(file)) >= 1)) {
      tableName <- "scc_stat"
      data <- vroom::vroom(file, ",", show_col_types = FALSE)
      data$stat_type <- strsplit(basename(file), "-")[[1]][[1]]
      data <- data %>% dplyr::rename(maximum = max, minimum = min)
    } else {
      next # Not handled results
    }
    ParallelLogger::logInfo("Inserting file ", file, "into table ", tableName)
    # TODO: this will not work for v large files. They will need to be split up
    # Regexp match files to upload to different tables
    DatabaseConnector::insertTable(connection,
                                   databaseSchema = config$resultsSchema,
                                   tableName = tableName,
                                   data = data,
                                   dropTableIfExists = FALSE,
                                   createTable = FALSE,
                                   tempTable = FALSE,
                                   bulkLoad = TRUE)
    if (cleanup) {
      unlink(file, recursive = TRUE, force = TRUE)
    }
  }
}

#' @title
#' Unzip and verify results zip with meta-data json
#' @description
#' Used to unzip and check all files in a zip folder with meta data file containing md5 hashes at time of creation
#' Used by both results generation and reference files
#' @param resultsZipPath zip file to inflate
#' @param unzipPath path to create
#' @param overwrite overwrite any existing
#' @export
unzipAndVerifyResultsZip <- function(resultsZipPath, unzipPath, overwrite = FALSE) {
  ParallelLogger::logInfo("Inflating zip archive ", resultsZipPath, " to ", unzipPath)
  if (!dir.exists(unzipPath)) {
    dir.create(unzipPath)
  }

  # Unzip full file
  utils::unzip(zipfile = resultsZipPath, exdir = unzipPath, overwrite = overwrite)
  # Perform checksum verifications
  metaFilePath <- file.path(unzipPath, "meta-info.json")
  checkmate::assert_file_exists(metaFilePath)
  hashList <- jsonlite::read_json(file.path(unzipPath, "meta-info.json"))

  ParallelLogger::logInfo(paste("Verifying file checksums"))
  # Check files are valid
  for (file in names(hashList)) {
    hash <- hashList[[file]]
    ParallelLogger::logInfo(paste("checking file hash", file, hash))
    unzipFile <- file.path(unzipPath, file)
    checkmate::assert_file_exists(unzipFile)
    verifyCheckSum <- tools::md5sum(unzipFile)[[1]]
    checkmate::assert_true(hash == verifyCheckSum)
  }

  return(lapply(names(hashList), function(file) { tools::file_path_as_absolute(file.path(unzipPath, file)) }))
}
