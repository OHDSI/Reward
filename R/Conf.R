# Copyright 2022 Observational Health Data Sciences and Informatics
#
# This file is part of CohortDiagnostics
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
#' @param globalConfigPath path to global yaml
#' @export
loadGlobalConfiguration <- function(globalConfigPath) {
  config <- yaml::read_yaml(globalConfigPath)

  defaults <- list(tables = list())
  config <- .setDefaultOptions(config, defaults)

  defaultTables <- list(
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

  config$tables <- .setDefaultOptions(config$tables, defaultTables)

  if (is.null(config$connectionDetails$user)) {
    user <- Sys.getenv("REWARD_DB_USER", "reward_user")
    config$connectionDetails$user <- user
  }


  if (config$connectionDetails$dbms != "sqlite") {
    if (is.null(config$connectionDetails$password)) {

      if (!is.null(config$keyringService)) {
        config$connectionDetails$password <- keyring::key_get(config$keyringService, username = config$connectionDetails$user)
      } else {
        stop("Set password securely with keyringService option and using keyring::key_set with the database username.")
      }
    }
  }
  config$connectionDetails <- do.call(DatabaseConnector::createConnectionDetails, config$connectionDetails)
  return(config)
}
