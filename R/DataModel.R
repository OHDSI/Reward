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

#' CEM Database Backend Class
#' @description
#' An interface to the common evidence model that uses works directly with a database schema
#' @field connection DatabaseConnector::connection instance
#' @field vocabularySchema OMOP vocabulary schema (must include concept and concept ancestor tables)
#' @field sourceSchema schema containing source_info table
#' @import checkmate
#' @import R6
#' @export
RewardDataModel <- R6::R6Class(
  "RewardDataModel",
  public = list(
    connection = NULL,
    config = NULL,
    vocabularySchema = NULL,
    resultsSchema = NULL,

    #' @description
    #' initialize backend object.
    #' @param config              Reward configuration object
    #' @param keyring             optional keyring::keyring object
    #' @param usePooledConnection Used a pooled connection object rather than a database connector object.
    initialize = function(configPath,
                          keyring = NULL,
                          usePooledConnection = FALSE) {

      self$config <- loadGlobalConfiguration(configPath, keyring = keyring)
      # Load connection
      if (usePooledConnection) {
        self$connection <- PooledConnectionHandler$new(self$config$connectionDetails)
      } else {
        self$connection <- ConnectionHandler$new(self$config$connectionDetails)
      }

      self$vocabularySchema <- self$config$vocabularySchema
      self$resultsSchema <- self$config$resultsSchema
    },

    #' @description
    #' Closes connection
    finalize = function() {
      self$connection$finalize()
    },

    getOutcomeCohortDefinitionSet = function(cohortIds = NULL) {
      # make a proper cohort definition set from sql and cohort json
      sql <- "SELECT cd.* FROM @results_schema.cohort_definition cd
      INNER JOIN @results_schema.outcome_cohort oc ON cd.cohort_definition_id = oc.cohort_definition_id
      {@cohort_ids != ''}? {WHERE cohort_definition_id IN (@cohort_ids)}
      "
      self$connection$queryDb(sql, cohort_ids = cohortIds, results_schema = self$resultsSchema)
    },

    getExposureCohortDefinitionSet = function(cohortIds = NULL) {
      # make a proper cohort definition set from sql and cohort json
      sql <- "SELECT cd.* FROM @results_schema.cohort_definition cd
      INNER JOIN @results_schema.exposure_cohort ec ON cd.cohort_definition_id = ec.cohort_definition_id
      {@cohort_ids != ''}? {WHERE cohort_definition_id IN (@cohort_ids)}
      "
      self$connection$queryDb(sql, cohort_ids = cohortIds, results_schema = self$resultsSchema)
    },

    getExposureCohortConceptSets = function(cohortIds = NULL) {
      sql <- "SELECT ccs.* FROM @results_schema.cohort_concept_set ccs
      INNER JOIN @results_schema.exposure_cohort ec ON ccs.cohort_definition_id = ec.cohort_definition_id
      {@cohort_ids != ''}? {WHERE cohort_definition_id IN (@cohort_ids)}
      "
      self$connection$queryDb(sql, cohort_ids = cohortIds, results_schema = self$resultsSchema)
    }
  )
)
