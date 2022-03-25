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

.setDefaultOptions <- function(config, defaults) {
  for (n in names(defaults)) {
    if (is.null(config[[n]])) {
      config[[n]] <- defaults[[n]]
    }
  }

  return(config)
}

#' Loads Application Context
#' @description
#' By default, loads the database connections in to this object
#' loads database password from prompt if REWARD_B_PASSWORD system env variable is not set (e.g. in .Rprofile)
#' The idea is to allow shared configuration settings between the web app and any data processing tools
#' @param configPath is a yaml file for the application configuration
#' @param globalConfigPath path to global yaml
#' @export
loadShinyAppContext <- function(configPath, globalConfigPath) {
  defaults <- list(
    useExposureControls = FALSE,
    custom_exposure_ids = c(),
    useConnectionPool = TRUE,
    analysisIds = c(1)
  )

  appContext <- .setDefaultOptions(yaml::read_yaml(configPath), defaults)
  appContext$globalConfig <- loadGlobalConfiguration(globalConfigPath)
  appContext$connectionDetails <- appContext$globalConfig$connectionDetails

  class(appContext) <- append(class(appContext), "appContext")
  return(appContext)
}

#' @title
#' Load report application context
#' @description
#' By default, loads the database connections in to this object
#' loads database password from prompt if REWARD_B_PASSWORD system env variable is not set (e.g. in .Rprofile)
#' The idea is to allow shared configuration settings between the web app and any data processing tools
#' @param globalConfigPath is a yaml file for the application configuratione
#' @param .env environment to load variable in to
#' @param exposureId exposure cohort id
#' @param outcomeId outcome cohort id
loadReportContext <- function(globalConfigPath) {
  reportAppContext <- loadGlobalConfiguration(globalConfigPath)
  reportAppContext$useConnectionPool <- TRUE
  class(reportAppContext) <- append(class(reportAppContext), "reportAppContext")
  return(reportAppContext)
}

#' Loads global config
#' @description
#' Load reward global config yaml file
#' @param globalConfigPath                          Path to global configuration yaml file
#' @param keyring                                   Optional keyring::keyring for storing secrets
#' @export
loadGlobalConfiguration <- function(globalConfigPath, keyring = NULL) {
  config <- yaml::read_yaml(globalConfigPath)

  defaults <- list(tables = list(), negativeControlCount = 150)
  config <- .setDefaultOptions(config, defaults)

  referenceTables <- list(
    referenceVersion = 'reference_version',
    cohortDefinition = 'cohort_definition',
    exposureCohort = 'exposure_cohort',
    outcomeCohort = 'outcome_cohort',
    cohortGroupDefinition = 'cohort_group_definition',
    cohortGroup = 'cohort_group',
    conceptSetDefinition = 'concept_set_definition',
    atlasCohortReference = 'atlas_cohort_reference',
    cohortConceptSet = 'cohort_concept_set',
    analysisSetting = 'analysis_setting'
  )

  config$referenceTables <- .setDefaultOptions(config$referenceTables, referenceTables)

  if (is.null(config$connectionDetails$user)) {
    user <- Sys.getenv("REWARD_DB_USER", "reward_user")
    config$connectionDetails$user <- user
  }


  if (config$connectionDetails$dbms != "sqlite") {
    if (is.null(config$connectionDetails$password)) {

      if (!is.null(config$keyringService)) {
        config$connectionDetails$password <- keyring::key_get(config$keyringService,
                                                              username = config$connectionDetails$user,
                                                              keyring = keyring)
      } else {
        stop("Set password securely with keyringService option and using keyring::key_set with the database username.")
      }
    }
  }
  config$connectionDetails <- do.call(DatabaseConnector::createConnectionDetails, config$connectionDetails)
  return(config)
}


#' Create global config file
#' @description
#' Create configuration file for REWARD server
#'
#' @inheritParams validateConfigFile
#' @param overwrite                     Overwite existing file (if it exists)
#' @export
createGlobalConfig <- function(configPath,
                               keyring = NULL,
                               overwrite = FALSE,
                               testConnection = TRUE) {

  # Copy default file
  defaultPath <- system.file("yml", "default.config.yml", package = utils::packageName())

  if (!base::file.exists(configPath) | overwrite) {
    ParallelLogger::logInfo("Creating new configuration file")
    base::file.copy(defaultPath, configPath)
  } else {
    ParallelLogger::logInfo("Editing existing file")
  }

  if (interactive()) {
    utils::file.edit(configPath)
    config <- yaml::read_yaml(configPath)
    if (!is.null(config$useSecurePassword)) {
      if (is.null(config$keyringService)) {
        stop("keyringService parameter must be set when using keyring")
      }

      user <- config$connectionDetails$user
      if (is.null(user)) {
        user <- Sys.getenv("REWARD_DB_USER", Sys.info()[["user"]])
      }

      tryCatch({
        keyring::key_get(config$keyringService, username = user, keyring = keyring)
        message("Password for", user, " with service ", config$keyringService, " already exists. Use keyring::keyset to change")
      },
        error = function(...) {
          message("Set keyring password for cdm user ", user, " with service ", config$keyringService)
          keyring::key_set(config$keyringService,
                           username = user,
                           keyring = keyring,
                           prompt = paste("Enter database password for user", user))
        })
    }

    validateConfigFile(configPath, testConnection = testConnection)
  } else {
    print("Default config file created at", configPath, "edit and call validateConfigFile to test.")
  }
}

#' Validate a cdm configuration file
#' @description
#' Opens a file for editing that contains the default settings for a cdm
#'
#' @inheritParams loadGlobalConfiguration
#' @param testConnection                Attempt to connect to database and write to schemas needed for writing?
#' @export
validateConfigFile <- function(configPath, testConnection = TRUE, keyring = NULL) {
  # Check required parameters exist
  requiredNames <- c("connectionDetails",
                     "webApiUrl",
                     "cemConnectionDetails",
                     "resultsSchema",
                     "exportPath")

  checkmate::assertFileExists(configPath)
  config <- yaml::read_yaml(configPath)
  checkmate::assertNames(names(config), must.include = requiredNames)
  ParallelLogger::logInfo("Required names are present.")

  if (testConnection) {
    config <- loadGlobalConfiguration(configPath, keyring = keyring)
    ParallelLogger::logInfo("Configuration loads, checking database connection")
    tryCatch({
      connection <- DatabaseConnector::connect(config$connectionDetails)
    }, error = function(msg) {
      stop(paste("Error with connection details could not connect to database"))
      ParallelLogger::logError(msg)
    })
    on.exit(DatabaseConnector::disconnect(connection))

    # Test schemas exists
    ParallelLogger::logInfo("Checking vocabulary schema")
    tryCatch({
      DatabaseConnector::renderTranslateQuerySql(connection,
                                                 "SELECT * FROM @vocabulary_schema.vocabulary",
                                                 vocabulary_schema = config$vocabularySchema)
    }, error = function(msg) {
      ParallelLogger::logError(msg)
      stop("Invalid vocabulary schema ", config$vocabularySchema)
    })

    ParallelLogger::logInfo("Checking results schemas")
    # Test schemas exists and are writable
    testSql <- "CREATE TABLE @schema.@test_table_name (id INTEGER); DROP TABLE @schema.@test_table_name;"

    tableName <- stringi::stri_rand_strings(1, 10, pattern = "[A-Za-z]")
    tryCatch({
      DatabaseConnector::renderTranslateExecuteSql(connection,
                                                   testSql,
                                                   progressBar = FALSE,
                                                   reportOverallTime = FALSE,
                                                   test_table_name = tableName,
                                                   schema = config$resultsSchema)
    }, error = function(msg) {
      ParallelLogger::logError(msg)
      stop("Invalid schema: ", config$resultsSchema, " cannot write table")
    })

    ParallelLogger::logInfo("Database configuration appears valid")
  }

  ParallelLogger::logInfo("Configuration appears valid")
}