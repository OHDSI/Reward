
# Add cohort method results
addCohortMethodResults <- function(resultsPath, schemaName) {
  ResultModelManager::uploadResults(
    connection = NULL,
    connectionDetails = NULL,
    schema,
    resultsFolder,
    tablePrefix = "",
    forceOverWriteOfSpecifications = FALSE,
    purgeSiteDataBeforeUploading = TRUE,
    databaseIdentifierFile = "cdm_source_info.csv",
    runCheckAndFixCommands = FALSE,
    warnOnMissingTable = TRUE,
    purgeDataModel = FALSE,
    specifications
  )

}