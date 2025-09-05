# Cohorts.R

# A. File Info -----------------------
# generate cohorts step
config <- config::get()
datasources <- config::get("datasources")
options(connectionObserver = NULL)
# Refresh cache
Reward::importRewardCohorts()
# Generate Cohorts--------------------------------------------------------------
cohortDefinitionSet <- Reward::getCohortDefinitionSet()

purrr::walk(datasources, function(datasource) {
  cli::cli_alert_info("Cohort execution on {datasource}")

  on.exit({
    gc()
    rm(list = ls())
  })

  # D. Variables -----------------------
  # make execution settings
  executionSettings <- config::get(config = datasource)
  connectionDetails <- do.call(DatabaseConnector::createConnectionDetails,
                               executionSettings$cdmConnectionDetails)

  # E. Script --------------------
  cohortTableNames <- CohortGenerator::getCohortTableNames(executionSettings$cohortTable)
  CohortGenerator::runCohortGeneration(
    connectionDetails,
    cdmDatabaseSchema = executionSettings$cdmDatabaseSchema,
    cohortDatabaseSchema = executionSettings$workDatabaseSchema,
    tempEmulationSchema = Sys.getenv('DATABRICKS_SCRATCH_SCHEMA'),
    cohortTableNames = cohortTableNames,
    cohortDefinitionSet = cohortDefinitionSet,
    stopOnError = FALSE,
    databaseId = executionSettings$databaseId,
    minCellCount = 5,
    incremental = TRUE,
    outputFolder = file.path("exec", "results", "cohorts", executionSettings$databaseId)
  )

})


purrr::walk(datasources, function(datasource) {

  executionSettings <- config::get(config = datasource)
  connectionDetails <- do.call(DatabaseConnector::createConnectionDetails,
                               executionSettings$cdmConnectionDetails)

  resultFolder <- file.path("exec", "results", "cohorts", executionSettings$databaseId)

  readr::write_csv(data.frame(database_id = executionSettings$databaseId),
                   file.path(resultFolder, "database_id.csv"))

  unlink(file.path(resultFolder, "cg_cohort_inclusion.csv"))
  # Upload results to main schema
  resultsConnectionDetails <- Reward::getResultsConnectionDetails(config = config)
  CohortGenerator::uploadResults(resultsConnectionDetails,
                                 schema = config$resultsDatabaseSchema,
                                 databaseIdentifierFile = "database_id.csv",
                                 resultsFolder = resultFolder,
                                 forceOverWriteOfSpecifications = FALSE,
                                 purgeSiteDataBeforeUploading = TRUE)
})

Reward::extractConceptSets(cohortDefinitionSet, config = config)
Reward::uploadConceptSets()