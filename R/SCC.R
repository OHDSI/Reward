# Interface to the SCC package

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
