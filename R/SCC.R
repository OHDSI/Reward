#' Generate a List of Self-Controlled Cohort Analysis Configurations
#'
#' Returns a predefined list of analysis settings for self-controlled cohort (SCC) studies, targeting default, pediatric, adult, and elderly populations.
#'
#' @details
#' This function constructs and returns a list of five SCC analyses. Each analysis is created using `SelfControlledCohort::createSccAnalysis()`, with variations in age strata. The run arguments for each include settings for exposure and outcome timing, age restrictions, and risk windows.
#'
#' The analyses returned cover:
#' - All ages, unstratified (default)
#' - Aged 18-30
#' - Pediatric (under 18)
#' - Aged 18-64
#' - Elderly (65+)
#'
#' Use this list to supply a set of population-specific SCC analyses when running functions like `execSccAnalyses()`.
#'
#' @return
#' A list of `SccAnalysis` objects, each containing an `analysisId`, `description`, and `runSelfControlledCohortArgs`.
#'
#' @examples
#' analyses <- getSccAnalysisList()
#' print(analyses[[1]]$description)  # "Default unstratified settings"
#'
#' @seealso [SelfControlledCohort::createSccAnalysis()], [execSccAnalyses()]
#' @export
getSccAnalysisList <- function() {
  list(
    SelfControlledCohort::createSccAnalysis(
      analysisId = 1,
      description = "Default unstratified settings",
      runSelfControlledCohortArgs = SelfControlledCohort::createRunSelfControlledCohortArgs(firstExposureOnly = TRUE,
                                                                                            firstOutcomeOnly = TRUE,
                                                                                            minAge = "",
                                                                                            maxAge = "",
                                                                                            studyStartDate = "",
                                                                                            studyEndDate = "",
                                                                                            addLengthOfExposureExposed = TRUE,
                                                                                            riskWindowStartExposed = 1,
                                                                                            riskWindowEndExposed = 30,
                                                                                            addLengthOfExposureUnexposed = TRUE,
                                                                                            riskWindowEndUnexposed = -1,
                                                                                            riskWindowStartUnexposed = -30,
                                                                                            hasFullTimeAtRisk = FALSE,
                                                                                            washoutPeriod = 0,
                                                                                            followupPeriod = 0)
    ),

    SelfControlledCohort::createSccAnalysis(
      analysisId = 2,
      description = "Aged 18 - 30",
      runSelfControlledCohortArgs = SelfControlledCohort::createRunSelfControlledCohortArgs(firstExposureOnly = TRUE,
                                                                                            firstOutcomeOnly = TRUE,
                                                                                            minAge = 18,
                                                                                            maxAge = 30,
                                                                                            studyStartDate = "",
                                                                                            studyEndDate = "",
                                                                                            addLengthOfExposureExposed = TRUE,
                                                                                            riskWindowStartExposed = 1,
                                                                                            riskWindowEndExposed = 30,
                                                                                            addLengthOfExposureUnexposed = TRUE,
                                                                                            riskWindowEndUnexposed = -1,
                                                                                            riskWindowStartUnexposed = -30,
                                                                                            hasFullTimeAtRisk = FALSE,
                                                                                            washoutPeriod = 0,
                                                                                            followupPeriod = 0)
    ),

    SelfControlledCohort::createSccAnalysis(
      analysisId = 3,
      description = "Under 18 (pediatrics)",
      runSelfControlledCohortArgs = SelfControlledCohort::createRunSelfControlledCohortArgs(firstExposureOnly = TRUE,
                                                                                            firstOutcomeOnly = TRUE,
                                                                                            minAge = "",
                                                                                            maxAge = 18,
                                                                                            studyStartDate = "",
                                                                                            studyEndDate = "",
                                                                                            addLengthOfExposureExposed = TRUE,
                                                                                            riskWindowStartExposed = 1,
                                                                                            riskWindowEndExposed = 30,
                                                                                            addLengthOfExposureUnexposed = TRUE,
                                                                                            riskWindowEndUnexposed = -1,
                                                                                            riskWindowStartUnexposed = -30,
                                                                                            hasFullTimeAtRisk = FALSE,
                                                                                            washoutPeriod = 0,
                                                                                            followupPeriod = 0)
    ),

    SelfControlledCohort::createSccAnalysis(
      analysisId = 4,
      description = "Aged 18 - 64",
      runSelfControlledCohortArgs = SelfControlledCohort::createRunSelfControlledCohortArgs(firstExposureOnly = TRUE,
                                                                                            firstOutcomeOnly = TRUE,
                                                                                            minAge = 18,
                                                                                            maxAge = 64,
                                                                                            studyStartDate = "",
                                                                                            studyEndDate = "",
                                                                                            addLengthOfExposureExposed = TRUE,
                                                                                            riskWindowStartExposed = 1,
                                                                                            riskWindowEndExposed = 30,
                                                                                            addLengthOfExposureUnexposed = TRUE,
                                                                                            riskWindowEndUnexposed = -1,
                                                                                            riskWindowStartUnexposed = -30,
                                                                                            hasFullTimeAtRisk = FALSE,
                                                                                            washoutPeriod = 0,
                                                                                            followupPeriod = 0)
    ),
    SelfControlledCohort::createSccAnalysis(
      analysisId = 5,
      description = "Aged 65+",
      runSelfControlledCohortArgs = SelfControlledCohort::createRunSelfControlledCohortArgs(firstExposureOnly = TRUE,
                                                                                            firstOutcomeOnly = TRUE,
                                                                                            minAge = 65,
                                                                                            maxAge = "",
                                                                                            studyStartDate = "",
                                                                                            studyEndDate = "",
                                                                                            addLengthOfExposureExposed = TRUE,
                                                                                            riskWindowStartExposed = 1,
                                                                                            riskWindowEndExposed = 30,
                                                                                            addLengthOfExposureUnexposed = TRUE,
                                                                                            riskWindowEndUnexposed = -1,
                                                                                            riskWindowStartUnexposed = -30,
                                                                                            hasFullTimeAtRisk = FALSE,
                                                                                            washoutPeriod = 0,
                                                                                            followupPeriod = 0)
    )
  )
}

#' Execute Scc analysis
#
#' @param connectionDetails An object containing details for connecting to the database, typically created using `DatabaseConnector::createConnectionDetails()`.
#' @param executionSettings A list containing execution environment metadata such as database ID, CDM schema, and work schema.
#' @param dashboard Optional dashboard configuration (default: NULL). Should contain settings and paths required for analysis.
#' @param analysisSettings A list of analysis settings as created by `getSccAnalysisList()`. Each element should include analysis parameters such as `runSelfControlledCohortArgs` and an `analysisId`.
#' @param config A configuration object, typically read from the global config using `config::get()`. Contains settings such as computeThreads.
#' @param exposureCohortIds Cohort IDs representing the exposures of interest. Defaults to all IDs returned by `getExposureCohortIds()`.
#' @param outcomeCohortIds Cohort IDs for outcomes of interest. Defaults to all IDs from `getOutcomeCohortIds()`.
#' @param negativeControls A table or data.frame of negative control pairs (`exposureId`, `outcomeId`). Optional. If not supplied or empty, results will not be calibrated.
#' @param controlType The type of controls to use when calibrating results. Default is `"outcome"`.
#'
#' @details
#' This function orchestrates running multiple SCC analyses in a specified environment. For each specified analysis, it:
#' - Checks for pre-existing results to avoid redundant computation.
#' - Assembles all required arguments including connection, cohort, and configuration information.
#' - Invokes `SelfControlledCohort::runSelfControlledCohort` for each set of analysis parameters.
#' - Issues CLI alerts to inform the user of progress and warnings (e.g., absence of negative controls).
#'
#' Results are stored to subfolders based on analysis identifiers and execution environment.
#'
#' @return This function is called for its side effects. Analytic results are exported to disk in result directories as determined by the supplied settings.
#'
#' @examples
#' \dontrun{
#' execSccAnalyses(
#'   connectionDetails = connectionDetailsObject,
#'   executionSettings = list(
#'     databaseId = "CDM_DB",
#'     cdmDatabaseSchema = "cdm_schema",
#'     workDatabaseSchema = "work_schema",
#'     cohortTable = "cohort"
#'   ),
#'   negativeControls = data.frame(exposureId = 1:5, outcomeId = 6:10),
#'   controlType = "outcome"
#' )
#' }
#'
#' @seealso [SelfControlledCohort::runSelfControlledCohort()]
#' @export
execSccAnalyses <- function(connectionDetails,
                            executionSettings,
                            dashboard = NULL,
                            analysisSettings = getSccAnalysisList(),
                            config = config::get(),
                            exposureCohortIds = getExposureCohortIds(),
                            outcomeCohortIds = getOutcomeCohortIds(),
                            negativeControls = NULL,
                            controlType = "outcome") {
  cli::cli_alert_info("Running scc on {executionSettings$databaseId}")

  if (length(negativeControls) == 0) {
    cli::cli_alert_warning("No negative controls found. Results will not be calibrated")
  }

  cohortTableNames <- CohortGenerator::getCohortTableNames(executionSettings$cohortTable)

  negativeControlsList <- purrr::pmap(negativeControls, function(exposureId, outcomeId, ...) {
    list(exposureId, outcomeId)
  })

  tableSpace <- dashboard$config$databaseSchema
  if (is.null(tableSpace))
    tableSpace <- "all_by_all"

  resultsPath <- file.path("exec", "results", executionSettings$databaseId, tableSpace, "scc_result")
  cli::cli_alert_info("Starting scc...")
  for (refRow in analysisSettings) {
    getrunSelfControlledCohortArgs <- refRow$runSelfControlledCohortArgs
    resultsExportPath <- file.path(resultsPath, paste0("A_", refRow$analysisId))

    if (file.exists(file.path(resultsExportPath, paste0("manifest.json")))) {
      cli::cli_alert_info("Results manifest found in {resultsExportPath} skipping analysis")
      next
    }

    args <- list(connectionDetails = connectionDetails,
                 cdmDatabaseSchema = executionSettings$cdmDatabaseSchema,
                 exposureDatabaseSchema = executionSettings$workDatabaseSchema,
                 resultsDatabaseSchema = executionSettings$workDatabaseSchema,
                 exposureTable = cohortTableNames$cohortTable,
                 outcomeDatabaseSchema = executionSettings$workDatabaseSchema,
                 outcomeTable = cohortTableNames$cohortTable,
                 exposureIds = exposureCohortIds,
                 outcomeIds = outcomeCohortIds,
                 databaseId = executionSettings$databaseId,
                 controlType = controlType,
                 negativeControlPairs = negativeControlsList,
                 # riskWindowsTable =  "reward_scc_risk_windows",
                 # resultsTable = "reward_scc_results",
                 analysisDescription = refRow$description,
                 analysisId = refRow$analysisId,
                 tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                 resultExportPath = resultsExportPath,
                 computeThreads = config$computeThreads)

    args <- append(args, getrunSelfControlledCohortArgs)
    do.call(SelfControlledCohort::runSelfControlledCohort, args)
  }
  cli::cli_alert_success("Scc analysis complete for {executionSettings$databaseId}")
}
