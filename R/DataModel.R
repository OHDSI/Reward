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
#' @field resultsSchema schema containing reward references and results
#' @field config    Reward configuration s3 object
#' @import checkmate
#' @import R6
#' @export
RewardDataModel <- R6::R6Class(
  "RewardDataModel",
  private = list(cemConnection = NULL),
  public = list(
    connection = NULL,
    config = NULL,
    vocabularySchema = NULL,
    resultsSchema = NULL,

        #' @description
        #' initialize backend object.
        #' @param configPath          Reward configuration yaml path
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

    #' @description
    #' Get cem connector api connection
    getCemConnection = function() {
      if (is.null(private$cemConnection)) {
        if (!is.null(self$config$cemConnectionDetails$connectionDetails)) {
          self$config$cemConnectionDetails$connectionDetails <- do.call(DatabaseConnector::createConnectionDetails,
                                                                        self$config$cemConnectionDetails$connectionDetails)
        }
        private$cemConnection <- do.call(CemConnector::createCemConnection, self$config$cemConnectionDetails)
      }
      # Return reference for reuse
      return(private$cemConnection)
    },

    getDataSources = function() {
      self$connection$queryDb("SELECT * FROM  @results_schema.data_source", results_schema = self$resultsSchema)
    },

    #' @description
    #' Get outcome cohort definition set
    #' @param cohortIds         numeric vector of cohort ids or null
    getOutcomeCohortDefinitionSet = function(cohortIds = NULL) {
      # make a proper cohort definition set from sql and cohort json
      sql <- "SELECT cd.* FROM @results_schema.cohort_definition cd
      INNER JOIN @results_schema.outcome_cohort oc ON cd.cohort_definition_id = oc.cohort_definition_id
      {@cohort_ids != ''}? {WHERE cohort_definition_id IN (@cohort_ids)}
      "
      self$connection$queryDb(sql, cohort_ids = cohortIds, results_schema = self$resultsSchema)
    },

        #' @description
        #' Get exposure cohort definition set
        #' @param cohortIds         numeric vector of cohort ids or null
    getExposureCohortDefinitionSet = function(cohortIds = NULL) {
      # make a proper cohort definition set from sql and cohort json
      sql <- "SELECT cd.* FROM @results_schema.cohort_definition cd
      INNER JOIN @results_schema.exposure_cohort ec ON cd.cohort_definition_id = ec.cohort_definition_id
      {@cohort_ids != ''}? {WHERE cohort_definition_id IN (@cohort_ids)}
      "
      self$connection$queryDb(sql, cohort_ids = cohortIds, results_schema = self$resultsSchema)
    },

        #' @description
        #' Get expsoure cohort concept sets
        #' @param cohortIds         numeric vector of cohort ids or null
    getExposureCohortConceptSets = function(cohortIds = NULL) {
      sql <- "SELECT ccs.* FROM @results_schema.cohort_concept_set ccs
      INNER JOIN @results_schema.exposure_cohort ec ON ccs.cohort_definition_id = ec.cohort_definition_id
      {@cohort_ids != ''}? {WHERE cohort_definition_id IN (@cohort_ids)}
      "
      self$connection$queryDb(sql, cohort_ids = cohortIds, results_schema = self$resultsSchema)
    },

    #' @description
    #' Get outcome cohort concept sets for all cohorts specified
    #' @param cohortIds         numeric vector of cohort ids or null
    getOutcomeCohortConceptSets = function(cohortIds = NULL) {
      sql <- "SELECT ccs.*, oc.outcome_type FROM @results_schema.cohort_concept_set ccs
      INNER JOIN @results_schema.outcome_cohort oc ON ccs.cohort_definition_id = oc.cohort_definition_id
      {@cohort_ids != ''}? {WHERE cohort_definition_id IN (@cohort_ids)}
      "
      self$connection$queryDb(sql, cohort_ids = cohortIds, results_schema = self$resultsSchema)
    },

    getCohortConceptSet = function(cohortDefinitionId) {
      sql <- "SELECT ccs.* FROM @results_schema.cohort_concept_set ccs
      WHERE cohort_definition_id = @cohort_definition_id"
      self$connection$queryDb(sql,
                              cohort_definition_id = cohortDefinitionId,
                              results_schema = self$resultsSchema)
    },

    #' @description
    #' Get getCohort data for one cohort
    #' @param cohortDefinitionId         cohort identifier (not null, integer)
    getCohort = function(cohortDefinitionId) {
      checkmate::assert_number(cohortDefinitionId)
      sql <- "SELECT cd.* FROM @results_schema.cohort_definition cd
      WHERE cohort_definition_id = @cohort_definition_id"
      cohortDf <- self$connection$queryDb(sql,
                                          cohort_definition_id = cohortDefinitionId,
                                          results_schema = self$resultsSchema)
      cohort <- setNames(split(cohortDf, seq(nrow(cohortDf))), rownames(cohortDf))[[1]]

      cohort$conceptSet <- self$getCohortConceptSet(cohortDefinitionId)
      return(cohort)
    },

    #' @description
    #' Get getCohortStats
    #' @param cohortDefinitionId         cohort identifier (not null, integer)
    #' @param isExposure                   Logical exposure cohort or not
    getCohortStats = function(cohortDefinitionId, isExposure) {
      checkmate::assert_number(cohortDefinitionId)
      checkmate::assert_logical(isExposure)
      sql <- "
      SELECT cd.cohort_definition_id,
             cd.short_name,
             ds.source_name,
             count(sccr.rr) as result_count
      FROM @results_schema.cohort_definition cd
      INNER JOIN @results_schema.scc_result sccr ON {@exposure} ? {sccr.target_cohort_id} : {sccr.outcome_cohort_id} = cd.cohort_definition_id
      INNER JOIN @results_schema.data_source ds ON ds.source_id = sccr.source_id
      WHERE cd.cohort_definition_id = @cohort_definition_id
      GROUP BY cd.cohort_definition_id, ds.source_name;"
      self$connection$queryDb(sql,
                              cohort_definition_id = cohortDefinitionId,
                              exposure = isExposure,
                              results_schema = self$resultsSchema)
    },

    #'
    #' Returns SCC results for a cohort id
    getNegativeControlSccResults = function(cohortDefinitionId,
                                             isExposure,
                                             outcomeType = NULL,
                                             conceptSet = NULL) {
      checkmate::assertLogical(isExposure)
      checkmate::assertIntegerish(outcomeType, null.ok = !isExposure)
      # Get negative controls from cem connection
      cemConnection <- self$getCemConnection()
      if (is.null(conceptSet))
        conceptSet <- self$getCohortConceptSet(cohortDefinitionId)

      if (isExposure) {
        controlConcepts <- cemConnection$getSuggestedControlCondtions(conceptSet)
      } else {
        controlConcepts <- cemConnection$getSuggestedControlIngredients(conceptSet)
      }

      if (nrow(controlConcepts)) {
        cohortIds <- controlConcepts$conceptId * 1000
        if (isExposure) {
          cohortIds <- cohortIds + outcomeType
        }

        sql <- "SELECT * FROM @results_schema.scc_result
        WHERE {@exposure} ? {outcome_cohort_id} : {target_cohort_id} IN (@cohort_ids)"

        res <- self$connection$queryDb(sql,
                                cohort_ids = cohortIds,
                                results_schema = self$resultsSchema,
                                exposure = isExposure)
        return(res)
      }
      return(data.frame())
    }
  )
)
