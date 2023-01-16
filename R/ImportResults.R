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

    # Handle table partitioning
    if (DatabaseConnector::dbms(connection) == "postgresql") {
      sql <- "
      CREATE TABLE @schema.scc_result_s@source_id
      PARTITION OF @schema.scc_result FOR VALUES IN (@source_id)
      PARTITION BY LIST (analysis_id);

      CREATE TABLE @schema.scc_stat_s@source_id
      PARTITION OF @schema.scc_stat FOR VALUES IN (@source_id)
      PARTITION BY LIST (analysis_id);
      "
      DatabaseConnector::renderTranslateExecuteSql(connection,
                                                   sql,
                                                   schema = config$resultsSchema,
                                                   source_id = sourceId)

      sql <- "SELECT analysis_id FROM @schema.analysis_setting"
      ids <- DatabaseConnector::renderTranslateQuerySql(connection,
                                                        sql,
                                                        schema = config$resultsSchema,
                                                        snakeCaseToCamelCase = TRUE) %>%
        dplyr::select(analysisId) %>%
        dplyr::pull()

      # Create an analysis id table
      for (id in ids) {
        sql <- "
        CREATE TABLE @schema.scc_result_s@source_id_a@analysis_id
        PARTITION OF @schema.scc_result_s@source_id FOR VALUES IN (@analysis_id);

        CREATE TABLE @schema.scc_stat_s@source_id_a@analysis_id
        PARTITION OF @schema.scc_stat_s@source_id FOR VALUES IN (@analysis_id);"
        DatabaseConnector::renderTranslateExecuteSql(connection,
                                                     sql,
                                                     analysis_id = id,
                                                     schema = config$resultsSchema,
                                                     source_id = sourceId)
      }
    }
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

#' Insert a set of files into DB from s3 bucket objects
#' @description
#' Creates a load table for insert of all objects.
#'
#' Checks insert of chunks by validating last entry of data file into table - if entry is found then data is not inserted
#' S3 Object is deleted when it is inserted or if it clashes with a primary key
#'
#' This caused the insert to succeed but the error code causes a crash in R
#'
#' Note, this function also works around a bug in readr/vroom whereby a temproary file is created when opening the
#' s3 bucket. This file is not deleted until the thread closes
#'
#' This file is the size of the csv.gz so quickly fills up the system's temp space which will lead to a catestrophic
#' failure on windows.
#'
#' Consequently, we set VROOM_TEMP_PATH to a customizable dir. This process is also lightning
#' fast if you use the ram as a disk store for the tempdir as DatabaseConnector also writes a tempfile.
#' On linux this is trivial but this can be achieved on windows server with some configuration:
#' @seealso http://woshub.com/create-ram-disk-windows-server/
#'
#'
#' Note the use of load per thread tables is because pgcopy is not threadsafe with inserts (i.e. there is no locking)
#' @return list of load tables (which are based on spawned proc ids) - to be merged by calling proc
uploadS3Files <- function(manifestDf, connectionDetails, targetSchema, cdmInfo) {
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  bucketInfo <- list()
  for (bucket in unique(manifestDf$bucket)) {
    bucketInfo[[bucket]] <- as.data.frame(aws.s3::get_bucket(bucket = bucket))$Key
  }

  tableId <- Sys.getpid()
  loadTables <- list(
    "scc_result" = list(
      keyCols = c("source_id", "analysis_id", "outcome_cohort_id", "target_cohort_id"),
      table = paste0("scc_result_load_table_", tableId)
    ),
    "scc_stat" = list(
      keyCols = c("source_id", "analysis_id", "outcome_cohort_id", "target_cohort_id", "stat_type"),
      table = paste0("scc_stat_load_table_", tableId)
    )
  )

  sql <- SqlRender::loadRenderTranslateSql(file.path("create", "loadTables.sql"),
                                           packageName = utils::packageName(),
                                           table_id = tableId,
                                           schema = targetSchema)

  DatabaseConnector::executeSql(connection, sql)

  tempd <- tempfile()
  dir.create(tempd)
  on.exit(unlink(tempd, TRUE, TRUE), add = TRUE)
  withr::with_envvar(list(VROOM_TEMP_PATH = tempd), {
    for (i in 1:nrow(manifestDf)) {
      fileRef <- manifestDf[i,]
      head <- fileRef$object %in% bucketInfo[[fileRef$bucket]]
      # Skip any removed objects - this means they have been inserted or there is an error we can't control
      if (isFALSE(head)) {
        ParallelLogger::logInfo("S3 object not found: ", fileRef$object)
        next
      }
      deleteObject <- FALSE
      tryCatch({
        ParallelLogger::logDebug("Loading chunk into file: ")
        chunk <- aws.s3::s3read_using(readr::read_csv,
                                      object = fileRef$object,
                                      bucket = fileRef$bucket)

        if (fileRef$target == "scc_result") {
          chunk <- chunk %>% dplyr::filter(!is.null(rr),
                                           rr > 0,
                                           !is.na(rr))
        }

        if (nrow(chunk)) {
          if (cdmInfo$changedSourceId) {
            chunk$source_id <- cdmInfo$sourceId
          }
          # Will use PGcopy to upload
          if (is.null(loadTables[[fileRef$target]])) {
            targetTable <- fileRef$target
          } else {
            targetTable <- loadTables[[fileRef$target]]$table
            # Get primary key columns for first element - if they are already in the db error out
            sql <- "SELECT @key_cols FROM @schema.@table WHERE @key_col_conditions"
            rowVals <- chunk[1,] %>% dplyr::select(dplyr::all_of(loadTables[[fileRef$target]]$keyCols))
            keyColConditions <- paste(loadTables[[fileRef$target]]$keyCols, " = ", rowVals, collapse = " AND ")
            testEntry <- DatabaseConnector::renderTranslateQuerySql(connection,
                                                                    sql,
                                                                    key_cols = loadTables[[fileRef$target]]$keyCols,
                                                                    key_col_conditions = keyColConditions,
                                                                    table = fileRef$target,
                                                                    schema = targetSchema)
            if (nrow(testEntry) != 0) {
              stop("key value violates unique constraint")
            }
          }

          DatabaseConnector::insertTable(connection,
                                         databaseSchema = targetSchema,
                                         tableName = targetTable,
                                         data = chunk,
                                         dropTableIfExists = FALSE,
                                         createTable = FALSE,
                                         tempTable = FALSE,
                                         bulkLoad = TRUE)
        }
        # delete file/chunk if upload success or it's empty
        deleteObject <- TRUE
      }, error = function(err) {
        ParallelLogger::logError("Error uploading ", fileRef$object, "\n", err)
        if (grepl("key value violates unique constraint", err)) {
          ParallelLogger::logInfo("Removing object due to primary key duplication")
          deleteObject <<- TRUE
        }
      }, finally = {
        # Manually cleanup temp cache so we don't fill disk up!
        rm(chunk)
        unlink(file.path(tempd, list.files(tempd)), force = TRUE, recursive = TRUE)
      })
      if (deleteObject) {
        ParallelLogger::logInfo("Removing object: ", fileRef$object)
        aws.s3::delete_object(fileRef$object, bucket = fileRef$bucket)
      }
    }
  })

  return(
    list(
      scc_result = paste0("scc_result_load_table_", tableId),
      scc_stat = paste0("scc_stat_load_table_", tableId)
    )
  )
}


#' Import Results from CDM
#' @description
#' Verify and import results exported from CDMs into S3 bucket(s)
#'
#' It is strongly advised that you use this utility on an EC2 box or elsewhere within the AWS cloud
#'
#' Requires aws.s3 package and configured environment variables:
#' AWS_SSE_TYPE="AES256"
#' AWS_BUCKET_NAME
#' AWS_OBJECT_KEY
#' AWS_ACCESS_KEY_ID
#' AWS_SECRET_ACCESS_KEY
#' AWS_DEFAULT_REGION
#'
#' @param config                        Reward configuration object
#' @param cdmManifest                   Path to cdm manifest json including location of s3 objects
#' @param connection                    DatabaseConnector connection to reward databases
#' @param computeStatsTables            compute tables that count aggregated stats across different data sources
#' @param numberOfThreads               Number of upload jobs to pull and uploads
#' @export
importResultsFromS3 <- function(config,
                                cdmManifestPath,
                                connection = NULL,
                                computeStatsTables = TRUE,
                                numberOfThreads = 12) {

  ParallelLogger::clearLoggers()
  ParallelLogger::addDefaultFileLogger(file.path(config$exportPath, "fileImportLogger.txt"))
  ParallelLogger::addDefaultConsoleLogger()
  # connect to database
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails = config$connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  }

  cdmManifest <- ParallelLogger::loadSettingsFromJson(cdmManifestPath)
  cdmInfo <- registerCdm(config, connection, cdmManifestPath)


  manifestDf <- as.data.frame(cdmManifest$manifest)

  ParallelLogger::logInfo("Starting insert")
  cluster <- ParallelLogger::makeCluster(numberOfThreads = numberOfThreads)
  on.exit(ParallelLogger::stopCluster(cluster), add = TRUE)

  # Fast load map - no constraints allows this to scale to a many threaded async data insert
  loadTableIds <- ParallelLogger::clusterApply(cluster,
                                               split(manifestDf, rep_len(1:numberOfThreads, nrow(manifestDf))),
                                               fun = uploadS3Files,
                                               connectionDetails = config$connectionDetails,
                                               targetSchema = config$resultsSchema,
                                               cdmInfo = cdmInfo)

  # map individiual tablers to where they're loaded
  loadTableRes <- list()
  for (res in loadTableIds) {
    for (table in names(res)) {
      loadTableRes[[table]] <- c(loadTableRes[[table]], res[[table]])
    }
  }

  # Merge results step - this will likely be slow. Parallel operation is a hindrance here
  # This part ensures inserts are distinct
  ParallelLogger::logInfo("MERGING RESULTS ...")
  for (table in names(loadTableRes)) {
    insertSql <- paste("INSERT INTO @schema.@table",
                       paste(paste0("SELECT * FROM @schema.", loadTableRes[[table]]), collapse = " UNION "))
    DatabaseConnector::renderTranslateExecuteSql(connection,
                                                 insertSql,
                                                 schema = config$resultsSchema,
                                                 table = table)
  }

  for (res in loadTableIds) {
    for (table in names(res)) {
      DatabaseConnector::renderTranslateExecuteSql(connection,
                                                   "
                                                   TRUNCATE TABLE @schema.@load_table;
                                                   DROP TABLE @schema.@load_table;
                                                   ",
                                                   schema = config$resultsSchema,
                                                   load_table = res[[table]])
    }
  }

  if (computeStatsTables) {
    computeSourceCounts(config, connection)
  }
}