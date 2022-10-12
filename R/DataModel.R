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
RewardDataModel <- R6::R6Class(
  "RewardDataModel",
  private = list(cemConnection = NULL),
  public = list(
    connection = NULL,
    config = NULL,
    vocabularySchema = NULL,
    resultsSchema = NULL,
    cemConnectionDetails = list(),

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
        self$connection <- ResultModelManager::PooledConnectionHandler$new(self$config$connectionDetails)
      } else {
        self$connection <- ResultModelManager::ConnectionHandler$new(self$config$connectionDetails)
      }

      self$vocabularySchema <- self$config$vocabularySchema
      self$resultsSchema <- self$config$resultsSchema
      self$cemConnectionDetails <- self$config$cemConnectionDetails
    },

                                #' @description
                                #' Closes connection
    finalize = function() {
      self$connection$finalize()
    },

                                #' @description
                                #' Get cem connector api connection
    getCemConnection = function() {
      tryCatch({
        if (is.null(private$cemConnection)) {
          if (!is.null(self$cemConnectionDetails$connectionDetails)) {
            self$cemConnectionDetails$connectionDetails <- do.call(DatabaseConnector::createConnectionDetails,
                                                                   self$cemConnectionDetails$connectionDetails)
          }
          private$cemConnection <- do.call(CemConnector::createCemConnection, self$cemConnectionDetails)
        }
      }, error = function(err, ...) {
        warning("CEM connector connection not loaded: ", err)
        private$cemConnection <- NULL
      })
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
      {@cohort_ids != ''}? {WHERE ccs.cohort_definition_id IN (@cohort_ids)}
      "
      self$connection$queryDb(sql, cohort_ids = cohortIds, results_schema = self$resultsSchema)
    },

                                #' @description
                                #' Get outcome cohort concept sets for all cohorts specified
                                #' @param cohortIds         numeric vector of cohort ids or null
    getOutcomeCohortConceptSets = function(cohortIds = NULL) {
      sql <- "SELECT ccs.*, oc.outcome_type FROM @results_schema.cohort_concept_set ccs
      INNER JOIN @results_schema.outcome_cohort oc ON ccs.cohort_definition_id = oc.cohort_definition_id
      {@cohort_ids != ''}? {WHERE ccs.cohort_definition_id IN (@cohort_ids)}
      "
      self$connection$queryDb(sql, cohort_ids = cohortIds, results_schema = self$resultsSchema)
    },

    getCohortConceptSet = function(cohortDefinitionId = NULL) {
      sql <- "SELECT ccs.* FROM @results_schema.cohort_concept_set ccs
      {@cohort_definition_id != ''} ? {WHERE cohort_definition_id = @cohort_definition_id}"
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

    getAnalysisSettings = function(decode = TRUE) {
      sql <- "SELECT * FROM @results_schema.analysis_setting"
      rows <- self$connection$queryDb(sql, results_schema = self$resultsSchema)
                                                            #' Decode raw base 64
      if (decode) {
        decoded <- c()
        for (i in 1:nrow(rows)) {
          decoded <- c(decoded, rawToChar(base64enc::base64decode(rows$options[i])))
        }
        rows$options <- decoded
      }

      return(rows)
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
             aset.analysis_name,
             aset.analysis_id,
             s.count as result_count
      FROM {@exposure} ? {@results_schema.scc_target_source_counts} : {@results_schema.scc_outcome_source_counts} s
      INNER JOIN @results_schema.data_source ds ON ds.source_id = s.source_id
      INNER JOIN @results_schema.analysis_setting aset ON s.analysis_id = aset.analysis_id
      INNER JOIN @results_schema.cohort_definition cd ON
       {@exposure} ? {s.target_cohort_id} : {s.outcome_cohort_id} = cd.cohort_definition_id
      WHERE cd.cohort_definition_id = @cohort_definition_id;"
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
        WHERE {@exposure} ? {target_cohort_id} : {outcome_cohort_id} = @cohort_definition_id
        AND {@exposure} ? {outcome_cohort_id} : {target_cohort_id} IN (@cohort_ids)
        AND rr IS NOT NULL"

        res <- self$connection$queryDb(sql,
                                       cohort_definition_id = cohortDefinitionId,
                                       cohort_ids = cohortIds,
                                       results_schema = self$resultsSchema,
                                       exposure = isExposure)
        return(res)
      }
      return(data.frame())
    },

    getNegativeControlConditions = function(cohortIds = NULL) {
      cemConnection <- self$getCemConnection()
      if (is.null(cemConnection))
        return(data.frame())

      # Get concept sets for all exposure cohorts
      # Use CemConnector to get control outcome concepts
      # This will take a long time with the web api utility
      suggestedControlConditions <- self$getExposureCohortConceptSets(cohortIds = cohortIds) %>%
        dplyr::group_by(.data$cohortDefinitionId) %>%
        dplyr::group_modify(
          ~cemConnection$getSuggestedControlCondtions(.x, nControls = self$config$negativeControlCount)) %>%
        # Map control outcome concepts to cohorts (* 1000)
        dplyr::mutate(outcomeCohortId = .data$conceptId * 1000, outcomeType = 0)

      # Map other outcome types
      suggestedControlConditions <- suggestedControlConditions %>%
        dplyr::bind_rows(suggestedControlConditions %>%
                           dplyr::mutate(outcomeCohortId = .data$outcomeCohortId + 1, outcomeType = 1)) %>%
        dplyr::bind_rows(suggestedControlConditions %>%
                           dplyr::mutate(outcomeCohortId = .data$outcomeCohortId + 2, outcomeType = 2))

      suggestedControlConditions
    },

    getNegativeControlExposures = function(cohortIds = NULL) {
      cemConnection <- self$getCemConnection()
      if (is.null(cemConnection))
        return(data.frame())

      # Get concept sets for all exposure cohorts
      # Use CemConnector to get control outcome concepts
      # This will take a long time with the web api utility
      suggestedControlExposures <- self$getOutcomeCohortConceptSets(cohortIds = cohortIds) %>%
        dplyr::group_by(.data$cohortDefinitionId) %>%
        dplyr::group_modify(
          ~cemConnection$getSuggestedControlIngredients(.x, nControls = self$config$negativeControlCount)) %>%
        dplyr::mutate(targetCohortId = .data$conceptId * 1000)
    },
    #' Count any query as subquery - (note: will be inneficient in many situations)
    #' @param query                 Sql query string
    #' @param ...                   @seealso `SqlRender::render`
    #' @paran render                Optional - call sqlrender to render first or not
    countQuery = function(query, ..., render = TRUE) {
      if (render) {
        query <- SqlRender::render(query, ...)
      }

      res <- self$connection$queryDb("SELECT count(*) as CNT FROM (@sub_query) AS qur", sub_query = query)
      return(res$cnt)
    }
  )
)

#' CEM Database Backend Class
#' @description
#' An interface to the common evidence model that uses works directly with a database schema
#' @field connection DatabaseConnector::connection instance
#' @field vocabularySchema OMOP vocabulary schema (must include concept and concept ancestor tables)
#' @field resultsSchema schema containing reward references and results
#' @field config    Reward configuration s3 object
#' @import checkmate
#' @import R6
DashboardDataModel <- R6::R6Class(
  "DashboardDataModel",
  inherit = RewardDataModel,
  public = list(
    initialize = function(dashboardConfigPath = NULL,
                          connectionDetails,
                          cemConnectionDetails = NULL,
                          resultDatabaseSchema = NULL,
                          usePooledConnection = TRUE) {
      stopifnot("Must specify either database schema or sqlite file " = connectionDetails$dbms != "sqlite" | !is.null(resultDatabaseSchema))
      self$config <- loadDashboardConfiguration(dashboardConfigPath)
      # Load connection
      if (usePooledConnection) {
        self$connection <- ResultModelManager::PooledConnectionHandler$new(connectionDetails)
      } else {
        self$connection <- ResultModelManager::ConnectionHandler$new(connectionDetails)
      }

      self$resultsSchema <- resultDatabaseSchema

      if (is.null(cemConnectionDetails)) {
        cemConnectionDetails <- list()
      }

      self$cemConnectionDetails <- cemConnectionDetails
    },

    getDataSourceInfo = function() {
      sql <- "SELECT * FROM @results_schema.data_source"
      self$connection$queryDb(sql, results_schema = self$resultsSchema)
    },

    getOutcomeCohorts = function() {
      sql <- "
      SELECT cd.*, ec.outcome_type FROM @results_schema.cohort_definition cd
      INNER JOIN @results_schema.outcome_cohort ec ON cd.cohort_definition_id = ec.cohort_definition_id
      ORDER BY short_name"
      self$connection$queryDb(sql, results_schema = self$resultsSchema)
    },

    getExposureCohorts = function() {
      sql <- "
      SELECT cd.*, ec.atc_flg FROM @results_schema.cohort_definition cd
      INNER JOIN @results_schema.exposure_cohort ec ON cd.cohort_definition_id = ec.cohort_definition_id
      ORDER BY short_name"
      self$connection$queryDb(sql, results_schema = self$resultsSchema)
    },

    getFilteredTableResultsQuery = function(benefitThreshold = 0.5,
                                            riskThreshold = 2.0,
                                            pValueCut = 0.05,
                                            requiredBenefitSources = NULL,
                                            filterByMeta = FALSE,
                                            outcomeCohortTypes = c(0, 1, 2, 3),
                                            calibrated = TRUE,
                                            benefitCount = 1,
                                            riskCount = 0,
                                            targetCohorts = NULL,
                                            outcomeCohorts = NULL,
                                            exposureClasses = NULL,
                                            orderByCol = NULL,
                                            ascending = NULL,
                                            limit = NULL,
                                            offset = NULL) {
      calibrated <- ifelse(calibrated, 1, 0)
      filterOutcomes <- length(outcomeCohortTypes) > 0

      query <- SqlRender::loadRenderTranslateSql(
        sqlFilename = file.path("dashboard", "mainTable.sql"),
        packageName = "Reward",
        risk = riskThreshold,
        benefit = benefitThreshold,
        p_cut_value = pValueCut,
        filter_outcome_types = filterOutcomes,
        outcome_types = outcomeCohortTypes,
        risk_count = riskCount,
        benefit_count = benefitCount,
        calibrated = calibrated,
        show_exposure_classes = !self$config$exposureDashboard,
        filter_by_meta_analysis = filterByMeta,
        outcome_cohort_length = length(outcomeCohorts) > 0,
        outcome_cohorts = outcomeCohorts,
        target_cohort_length = length(targetCohorts) > 0,
        target_cohorts = targetCohorts,
        exposure_classes = exposureClasses,
        required_benefit_sources = requiredBenefitSources,
        required_benefit_count = length(requiredBenefitSources),
        order_by = orderByCol,
        ascending = ascending,
        limit = limit,
        offset = offset,
        schema = self$resultsSchema
      )
      return(query)
    },

    getFilteredTableResults = function(...) {
      sql <- self$getFilteredTableResultsQuery(...)
      self$connection$queryDb(sql)
    },

    getFilteredTableResultsCount = function(...) {
      sql <- self$getFilteredTableResultsQuery(...)
      self$countQuery(sql, render = FALSE)
    }
  )
)
