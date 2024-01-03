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

#' RewardDataModel
#' @description
#' An interface to the common evidence model that uses works directly with a database schema
#' @field connection DatabaseConnector::connection instance
#' @field vocabularySchema OMOP vocabulary schema (must include concept and concept ancestor tables)
#' @field resultsSchema schema containing reward references and results
#' @field config    Reward configuration s3 object
#' @field cemConnectionDetails  Cem COnnector connection details object
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
      self$config <-
        loadGlobalConfiguration(configPath, keyring = keyring)
      # Load connection
      if (usePooledConnection) {
        self$connection <-
          ResultModelManager::PooledConnectionHandler$new(self$config$connectionDetails)
      } else {
        self$connection <-
          ResultModelManager::ConnectionHandler$new(self$config$connectionDetails)
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
            self$cemConnectionDetails$connectionDetails <-
              do.call(
                DatabaseConnector::createConnectionDetails,
                self$cemConnectionDetails$connectionDetails
              )
          }
          private$cemConnection <-
            do.call(CemConnector::createCemConnection,
                    self$cemConnectionDetails)
        }
      }, error = function(err, ...) {
        warning("CEM connector connection not loaded: ", err)
        private$cemConnection <- NULL
      })
      # Return reference for reuse
      return(private$cemConnection)
    },
    #' Query database
    #' @param sql     query string
    #' @param results_schema      (optional) schema string
    #' @param ... @seealso `SqlRender::render`
    queryDb = function(sql, results_schema = config$resultsSchema, ...) {
      self$connection$queryDb(sql, results_schema = self$resultsSchema, ...)
    },

    #' Get Data Sources available
    #' @description
    #' List of data sources
    #' @returns data.frame of data sources
    #'
    getDataSources = function() {
      self$queryDb("SELECT * FROM  @results_schema.data_source")
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
      self$connection$queryDb(sql,
                              cohort_ids = cohortIds,
                              results_schema = self$resultsSchema)
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
      self$connection$queryDb(sql,
                              cohort_ids = cohortIds,
                              results_schema = self$resultsSchema)
    },

    #' @description
    #' Get expsoure cohort concept sets
    #' @param cohortIds         numeric vector of cohort ids or null
    getExposureCohortConceptSets = function(cohortIds = NULL) {
      sql <- "SELECT ccs.* FROM @results_schema.cohort_concept_set ccs
      INNER JOIN @results_schema.exposure_cohort ec ON ccs.cohort_definition_id = ec.cohort_definition_id
      {@cohort_ids != ''}? {WHERE ccs.cohort_definition_id IN (@cohort_ids)}
      "
      self$connection$queryDb(sql,
                              cohort_ids = cohortIds,
                              results_schema = self$resultsSchema)
    },

    #' @description
    #' Get outcome cohort concept sets for all cohorts specified
    #' @param cohortIds         numeric vector of cohort ids or null
    getOutcomeCohortConceptSets = function(cohortIds = NULL) {
      sql <-
        "SELECT ccs.*, oc.outcome_type FROM @results_schema.cohort_concept_set ccs
      INNER JOIN @results_schema.outcome_cohort oc ON ccs.cohort_definition_id = oc.cohort_definition_id
      {@cohort_ids != ''}? {WHERE ccs.cohort_definition_id IN (@cohort_ids)}
      "
      self$connection$queryDb(sql,
                              cohort_ids = cohortIds,
                              results_schema = self$resultsSchema)
    },

    #' Get concept set for cohort
    #'
    #' @param cohortDefinitionId  Cohort definition id
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
      cohort <-
        setNames(split(cohortDf, seq(nrow(cohortDf))), rownames(cohortDf))[[1]]

      cohort$conceptSet <-
        self$getCohortConceptSet(cohortDefinitionId)
      return(cohort)
    },

    #' Get analysis settings
    #'
    #' @param decode convert json to r list
    getAnalysisSettings = function(decode = TRUE) {
      sql <- "SELECT * FROM @results_schema.analysis_setting"
      rows <-
        self$connection$queryDb(sql, results_schema = self$resultsSchema)
      #' Decode raw base 64
      if (decode) {
        decoded <- c()
        for (i in 1:nrow(rows)) {
          decoded <-
            c(decoded, rawToChar(base64enc::base64decode(rows$options[i])))
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
      self$connection$queryDb(
        sql,
        cohort_definition_id = cohortDefinitionId,
        exposure = isExposure,
        results_schema = self$resultsSchema
      )
    },
    #' Returns SCC results for a cohort id
    #'
    #' @param cohortDefinitionId          Cohort Id
    #' @param outcomeType               Outcome types
    #' @param conceptSet                conceptset type
    #' @param isExposure                  Boolean - is this for exposure cohort?
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
        controlConcepts <-
          cemConnection$getSuggestedControlCondtions(conceptSet)
      } else {
        controlConcepts <-
          cemConnection$getSuggestedControlIngredients(conceptSet)
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

        res <- self$connection$queryDb(
          sql,
          cohort_definition_id = cohortDefinitionId,
          cohort_ids = cohortIds,
          results_schema = self$resultsSchema,
          exposure = isExposure
        )
        return(res)
      }
      return(data.frame())
    },

    #' Get negative control condition rr values
    #' @param cohortIds     (Optional) List of cohort identifiers
    getNegativeControlConditions = function(cohortIds = NULL) {
      cemConnection <- self$getCemConnection()
      if (is.null(cemConnection))
        return(data.frame())

      # Get concept sets for all exposure cohorts
      # Use CemConnector to get control outcome concepts
      # This will take a long time with the web api utility
      suggestedControlConditions <-
        self$getExposureCohortConceptSets(cohortIds = cohortIds) %>%
        dplyr::group_by(cohortDefinitionId) %>%
        dplyr::group_modify(
          ~ cemConnection$getSuggestedControlCondtions(.x, nControls = self$config$negativeControlCount)
        ) %>%
        # Map control outcome concepts to cohorts (* 1000)
        dplyr::mutate(outcomeCohortId = conceptId * 1000, outcomeType = 0)

      # Map other outcome types
      suggestedControlConditions <- suggestedControlConditions %>%
        dplyr::bind_rows(
          suggestedControlConditions %>%
            dplyr::mutate(outcomeCohortId = outcomeCohortId + 1, outcomeType = 1)
        ) %>%
        dplyr::bind_rows(
          suggestedControlConditions %>%
            dplyr::mutate(outcomeCohortId = outcomeCohortId + 2, outcomeType = 2)
        )

      suggestedControlConditions
    },
    #' Get negative control exposures
    #' @param cohortIds     (Optional) List of cohort identifiers
    getNegativeControlExposures = function(cohortIds = NULL) {
      cemConnection <- self$getCemConnection()
      if (is.null(cemConnection))
        return(data.frame())

      # Get concept sets for all exposure cohorts
      # Use CemConnector to get control outcome concepts
      # This will take a long time with the web api utility
      suggestedControlExposures <-
        self$getOutcomeCohortConceptSets(cohortIds = cohortIds) %>%
        dplyr::group_by(cohortDefinitionId) %>%
        dplyr::group_modify(
          ~ cemConnection$getSuggestedControlIngredients(.x, nControls = self$config$negativeControlCount)
        ) %>%
        dplyr::mutate(targetCohortId = conceptId * 1000)
    },
    #' Count any query as subquery - (note: will be inneficient in many situations)
    #' @param query                 Sql query string
    #' @param ...                   @seealso `SqlRender::render`
    #' @param render                Optional - call sqlrender to render first or not
    countQuery = function(query, ..., render = TRUE) {
      if (render) {
        query <- SqlRender::render(query, ...)
      }

      res <-
        self$connection$queryDb("SELECT count(*) as CNT FROM (@sub_query) AS qur", sub_query = query)
      return(res$cnt)
    }
  )
)

#' CEM Database Backend Class
#' @description
#' An interface to the common evidence model that uses works directly with a database schema
DashboardDataModel <- R6::R6Class(
  "DashboardDataModel",
  inherit = RewardDataModel,
  public = list(
    #'
    #'
    #' @param dashboardConfigPath     Path to dashboart yaml file
    #' @param connectionDetails       Database Connector connection detils
    #' @param cemConnectionDetails    Cem Connection details
    #' @param resultDatabaseSchema    Results database schema
    #' @param vocabularyDatabaseSchema vocab database schema
    #' @param usePooledConnection     Use a poooled database connection
    initialize = function(dashboardConfigPath = NULL,
                          connectionDetails,
                          cemConnectionDetails = NULL,
                          resultDatabaseSchema = NULL,
                          vocabularyDatabaseSchema = resultDatabaseSchema,
                          usePooledConnection = TRUE) {
      stopifnot(
        "Must specify either database schema or sqlite file " = connectionDetails$dbms != "sqlite" |
          !is.null(resultDatabaseSchema)
      )
      self$config <- loadDashboardConfiguration(dashboardConfigPath)
      # Load connection
      if (usePooledConnection) {
        self$connection <-
          ResultModelManager::PooledConnectionHandler$new(connectionDetails)
      } else {
        self$connection <-
          ResultModelManager::ConnectionHandler$new(connectionDetails)
      }

      self$resultsSchema <- resultDatabaseSchema

      if (is.null(cemConnectionDetails)) {
        cemConnectionDetails <- list()
      }

      self$vocabularySchema <- vocabularyDatabaseSchema
      self$cemConnectionDetails <- cemConnectionDetails
    },

    #' Get Outcome Cohorts
    #'
    #' @return data.frame of cohorts
    getOutcomeCohorts = function() {
      sql <- "
      SELECT cd.*, ec.outcome_type FROM @results_schema.cohort_definition cd
      INNER JOIN @results_schema.outcome_cohort ec ON cd.cohort_definition_id = ec.cohort_definition_id
      ORDER BY short_name"
      self$connection$queryDb(sql, results_schema = self$resultsSchema)
    },

    #' Get Exposure Cohorts
    #'
    #' @return data.frame of cohorts
    getExposureCohorts = function() {
      sql <- "
      SELECT cd.*, ec.atc_flg FROM @results_schema.cohort_definition cd
      INNER JOIN @results_schema.exposure_cohort ec ON cd.cohort_definition_id = ec.cohort_definition_id
      ORDER BY short_name"
      self$connection$queryDb(sql, results_schema = self$resultsSchema)
    },

    #' Get exposure class names
    #' @return string of names for exposure classes
    getExposureClassNames = function() {
      # TODO: exposure class table currently not generated
      return(c())
    },

    #' Shiny Dashboard - main query
    #'
    #' @param benefitThreshold thereshold to consider a benefit
    #' @param lowerBenefitThereshold thereshold to consider a benefit
    #'        (lower bounds, e.g. exclude benefits below 0.1 to rule out potential indications)
    #' @param riskThreshold threshold to consider effect estimate a risk
    #' @param pValueCut pvalue hacking
    #' @param requiredBenefitSources required sources to be in results
    #' @param filterByMeta filter by meta analysis RR?
    #' @param outcomeCohortTypes outcome cohorts to filter by type of
    #' @param calibrated calibrated result
    #' @param benefitCount minimum number of benefits found
    #' @param riskCount number of accerptable risks
    #' @param targetCohorts target cohort ids
    #' @param outcomeCohorts outcome cohort ids
    #' @param exposureClasses exposureClasses filter
    #' @param orderByCol Order by which column?
    #' @param ascending ascending order?
    #' @param limit Row limit
    #' @param offset Row Offset
    #' @param excludedConcepts concept id's to exclude from results
    getFilteredTableResultsQuery = function(benefitThreshold = 0.5,
                                            lowerBenefitThereshold = 0.0,
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
                                            excludedConcepts = NULL,
                                            limit = NULL,
                                            offset = NULL) {
      calibrated <- ifelse(calibrated, 1, 0)
      filterOutcomes <- length(outcomeCohortTypes) > 0

      query <- SqlRender::loadRenderTranslateSql(
        sqlFilename = file.path("dashboard", "mainTable.sql"),
        packageName = "Reward",
        risk = riskThreshold,
        lower_benefit = lowerBenefitThereshold,
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
        excluded_concepts = excludedConcepts,
        vocabulary_schema = self$vocabularySchema,
        order_by = orderByCol,
        ascending = ascending,
        limit = limit,
        offset = offset,
        schema = self$resultsSchema
      )
      return(query)
    },

    #'
    #' Download full data set of  RR values
    #' @return
    getFullDataSet = function() {
      sql <- "
      SELECT t.short_name as target_cohort, o.short_name, ds.source_name, r.*

      FROM @results_schema.scc_result r
      INNER JOIN @results_schema.cohort_definition t ON t.cohort_definition_id = r.target_cohort_id
      INNER JOIN @results_schema.cohort_definition o ON o.cohort_definition_id = r.outcome_cohort_id
      INNER JOIN @results_schema.data_source ds ON ds.source_id = r.source_id
      WHERE r.RR <= 1.0 -- Hard coded risk threshold required
      "
      self$queryDb(sql)
    },

    #' Main table query in dashboard
    #'
    #' @param ...
    #'
    #' @return
    #'
    getFilteredTableResults = function(...) {
      sql <- self$getFilteredTableResultsQuery(...)
      self$connection$queryDb(sql)
    },

    #' Get main query results count
    #' @param ...
    #'
    #' @return count
    getFilteredTableResultsCount = function(...) {
      sql <- self$getFilteredTableResultsQuery(...)
      self$countQuery(sql, render = FALSE)
    },

    #' Get table of meta analysis results
    #'
    #' @param exposureId exposure cohort id
    #' @param outcomeId outcome Cohort id
    getMetaAnalysisTable = function(exposureId, outcomeId) {
      sql <- "
        SELECT r.SOURCE_ID,
            ds.SOURCE_NAME,
            r.RR,
            CONCAT(ROUND(r.LB_95, 2), '-', ROUND(r.UB_95, 2)) AS CI_95,
            r.LB_95,
            r.UB_95,
            r.P_VALUE,
            r2.RR as Calibrated_RR,
            CONCAT(ROUND(r2.LB_95, 2) , '-', ROUND(r2.UB_95, 2)) AS CALIBRATED_CI_95,
            r2.LB_95 as CALIBRATED_LB_95,
            r2.UB_95 as CALIBRATED_UB_95,
            r2.P_VALUE as Calibrated_P_VALUE,
            r.C_AT_RISK,
            r.C_PT,
            r.C_CASES,
            r.T_AT_RISK,
            r.T_PT,
            r.T_CASES
        FROM @results_schema.scc_result r
        INNER JOIN @results_schema.data_source ds ON ds.source_id = r.source_id
        LEFT JOIN @results_schema.scc_result r2 ON (
                r2.OUTCOME_COHORT_ID = r.OUTCOME_COHORT_ID
                AND r2.TARGET_COHORT_ID = r.TARGET_COHORT_ID
                AND r2.calibrated = 1
                AND r2.source_id = r.source_id
            )
            WHERE r.OUTCOME_COHORT_ID = @outcome
            AND r.TARGET_COHORT_ID = @treatment
            AND r.calibrated = 0
        ORDER BY r.SOURCE_ID
      "
      return(self$queryDb(sql, treatment = exposureId, outcome = outcomeId))
    },

    #' Get Forest plot table
    #'
    #' @param exposureId  exposure Id
    #' @param outcomeId   outcome Id
    #' @param calibrated  get calibrated results?

    getForestPlotTable = function(exposureId, outcomeId, calibrated) {
      sql <- "
      {DEFAULT @use_calibration = TRUE}
      SELECT
          r.SOURCE_ID,
          ds.SOURCE_NAME,
          r.C_AT_RISK,
          r.C_PT,
          r.C_CASES,
          r.RR,
          r.LB_95,
          r.UB_95,
          r.P_VALUE,
          r.T_AT_RISK,
          r.T_PT,
          r.T_CASES,
          r.SE_LOG_RR,
          {@use_calibration} ? { r.calibrated, }
          r.I2
      FROM @results_schema.scc_result r
      INNER JOIN @results_schema.data_source ds ON ds.source_id = r.source_id
          WHERE r.OUTCOME_COHORT_ID = @outcome
          AND r.TARGET_COHORT_ID = @treatment
          {@use_calibration} ? { AND r.calibrated IN (@calibrated) }
      ORDER BY r.SOURCE_ID
      "
      table <-
        self$queryDb(sql,
                     treatment = exposureId,
                     outcome = outcomeId,
                     calibrated = calibrated)
      calibratedTable <- table %>% dplyr::filter(calibrated == 1)
      uncalibratedTable <- table %>% dplyr::filter(calibrated == 0)

      if (nrow(calibratedTable) & nrow(uncalibratedTable)) {
        calibratedTable$calibrated <- "Calibrated"
        uncalibratedTable$calibrated <- "Uncalibrated"
        uncalibratedTable$sourceName <-
          paste0(uncalibratedTable$sourceName, "\n uncalibrated")
        calibratedTable$sourceName <-
          paste0(calibratedTable$sourceName, "\n Calibrated")
      }

      table <-
        rbind(uncalibratedTable[order(uncalibratedTable$sourceId, decreasing = TRUE), ],
              calibratedTable[order(calibratedTable$sourceId, decreasing = TRUE), ])
      return(table)
    },


    #' Get Summary Statistics
    #'
    #' @param statType        statistic type
    #' @param exposureId      exposure cohort id
    #' @param outcomeId       outcome cohort id
    #' @param sourceIds       cohort source ids
    #' @param tableName       table name
    #' @param analysisId      Analysis setting id
    getSummaryStats = function(statType,
                               exposureId,
                               outcomeId,
                               sourceIds = NULL,
                               tableName = "scc_stat",
                               analysisId = 1) {
      self$queryDb(
        "
      SELECT
        ds.source_name,
        round(mean, 3) as mean,
        round(sd, 3) as sd,
        minimum as min,
        p_10 as p10,
        p_25 as p25,
        median as median,
        p_75 as p75,
        p_90 as p90,
        maximum as max,
        total
      FROM @results_schema.@table_name tts
      INNER JOIN @results_schema.data_source ds ON tts.source_id = ds.source_id
      WHERE stat_type = '@stat_type'
      AND target_cohort_id = @treatment AND outcome_cohort_id = @outcome
      AND mean is not NULL
      AND analysis_id = @analysis_id
      {@source_ids != ''} ? {AND ds.source_id IN (@source_ids)}",
        stat_type = statType,
        analysis_id = analysisId,
        table_name = tableName,
        treatment = exposureId,
        outcome = outcomeId,
        source_ids = sourceIds
      )
    },

    #' getTimeOnTreatmentStats
    #'
    #' @param ...
    getTimeToOutcomeStats = function(...) {
      self$getSummaryStats(statType = "time_to_outcome", ...)
    },

    #' getTimeOnTreatmentStats
    #'
    #' @param ...
    getTimeOnTreatmentStats = function(...) {
      self$getSummaryStats(statType = "time_on_treatment", ...)
    },

    #' Get Negative controls Scc results
    #'
    #' @param cohortDefinitionId    cohort id
    #' @param isExposure      is exposure or outcome?
    #' @param outcomeType     Outcome Type
    #' @param conceptSet      consept set data.frame
    getNegativeControlSccResults = function(cohortDefinitionId,
                                            isExposure,
                                            outcomeType = NULL,
                                            conceptSet = NULL) {
      checkmate::assertLogical(isExposure)
      checkmate::assertIntegerish(outcomeType, null.ok = !isExposure)

      sql <- "
      SELECT sr.* FROM @results_schema.scc_result sr
      INNER JOIN @results_schema.negative_control nc ON (
      nc.cohort_definition_id = {@exposure} ? {
        sr.target_cohort_id AND sr.outcome_cohort_id = nc.negative_control_cohort_id
        } : {
        sr.outcome_cohort_id AND sr.target_cohort_id = nc.negative_control_cohort_id
        }
      )
      {@exposure} ? {
        INNER JOIN @results_schema.outcome_cohort oc ON (
          oc.cohort_definition_id = sr.outcome_cohort_id AND oc.outcome_type = @outcome_type
        )
      }
      WHERE {@exposure} ? {sr.target_cohort_id} : {sr.outcome_cohort_id} = @cohort_definition_id
      AND sr.calibrated = 0
      AND rr IS NOT NULL"

      res <- self$queryDb(sql,
                          cohort_definition_id = cohortDefinitionId,
                          exposure = isExposure)
      return(res)

      return(data.frame())
    }
  )
)
