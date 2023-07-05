globalConfig <- Reward::loadGlobalConfiguration("reward-cf.yml")

ROhdsiWebApi::authorizeWebApi(globalConfig$webApiUrl, )

migraineCohortId <- 3
migraineWithIndexCorrectionCohortId <- 2
epilepsyCohortId <- 1

migrainCohortSubsetDefinition <- CohortGenerator::createCohortSubsetDefinition(
  name = "Migraine Indication",
  definitionId = 1,
  subsetOperators = list(
    CohortGenerator::createCohortSubset(
      name = "migraine",
      cohortIds = c(migraineCohortId),
      cohortCombinationOperator = "any",
      negate = FALSE,
      startWindow = CohortGenerator::createSubsetCohortWindow(
        startDay = -365,
        endDay = 0,
        targetAnchor = "cohortStart"
      ),
      endWindow = CohortGenerator::createSubsetCohortWindow(
        startDay = 0,
        endDay = 99999,
        targetAnchor = "cohortStart"
      )
    )
  )
)


migrainIdCohortSubsetDefinition <- CohortGenerator::createCohortSubsetDefinition(
  name = "Migraine Indication (with index correction)",
  definitionId = 1,
  subsetOperators = list(
    CohortGenerator::createCohortSubset(
      name = "migraine - inex corrected",
      cohortIds = c(migraineWithIndexCorrectionCohortId),
      cohortCombinationOperator = "any",
      negate = FALSE,
      startWindow = CohortGenerator::createSubsetCohortWindow(
        startDay = -365,
        endDay = 0,
        targetAnchor = "cohortStart"
      ),
      endWindow = CohortGenerator::createSubsetCohortWindow(
        startDay = 0,
        endDay = 99999,
        targetAnchor = "cohortStart"
      )
    )
  )
)

epilepsyCohortSubsetDefinition <- CohortGenerator::createCohortSubsetDefinition(
  name = "Epilepsy Indication",
  definitionId = 1,
  subsetOperators = list(
    CohortGenerator::createCohortSubset(
      name = "Epilepsy",
      cohortIds = c(epilepsyCohortId),
      cohortCombinationOperator = "any",
      negate = FALSE,
      startWindow = CohortGenerator::createSubsetCohortWindow(
        startDay = -365,
        endDay = 0,
        targetAnchor = "cohortStart"
      ),
      endWindow = CohortGenerator::createSubsetCohortWindow(
        startDay = 0,
        endDay = 99999,
        targetAnchor = "cohortStart"
      )
    )
  )
)


Reward::addSubsetDefinition(tConnection, tConfig, cohortSubsetDefinition = migrainCohortSubsetDefinition, targetCohortIds = c(742267000))
Reward::addSubsetDefinition(tConnection, tConfig, cohortSubsetDefinition = migrainIdCohortSubsetDefinition, targetCohortIds = c(742267000))
Reward::addSubsetDefinition(tConnection, tConfig, cohortSubsetDefinition = epilepsyCohortSubsetDefinition, targetCohortIds = c(742267000))


importReferences <- function(cdmConfigPath, referenceZipFile = "test-reward-refs.zip") {
  config <- RewardExecutionPackage::loadCdmConfiguration(cdmConfigPath)
  connection <- DatabaseConnector::connect(config$connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  RewardExecutionPackage::importReferenceTables(connection, config, referenceZipFile, overwriteReferences = TRUE)
}


createCohorts <- function(cdmConfigPath) {
  config <- RewardExecutionPackage::loadCdmConfiguration(cdmConfigPath)
  connection <- DatabaseConnector::connect(config$connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  RewardExecutionPackage::generateAtlasCohortSet(config, connection)
}

computeSccStudy <- function(cdmConfigPath, referenceZipFile = "test-reward-refs.zip") {
  config <- RewardExecutionPackage::loadCdmConfiguration(cdmConfigPath)
  connection <- DatabaseConnector::connect(config$connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  baseDb <- config$database
  config$awsS3Log <- paste0("upload_log-", config$database, ".csv")
  RewardExecutionPackage::computeSccResults(connection, config, targetCohortIds = c(4, 5, 6))

}

configs <- file.path("cdmConfig", list.files("cdmConfig", "yml"))

for (configPath in configs) {
  importReferences(configPath)
}

for (configPath in configs) {
  createCohorts(configPath)
}

for (configPath in configs) {
  computeSccStudy(configPath)
}

insertStudyResults <- function(cdmConfigPath) {
  config <- RewardExecutionPackage::loadCdmConfiguration(cdmConfigPath)
  baseDb <- config$database
  globalConfig <- Reward::loadGlobalConfiguration("reward-cf.yml")

  Reward::importResultsFromS3(globalConfig,
                              numberOfThreads = 12,
                              cdmManifestPath = file.path(config$exportPath,
                                                          paste0(config$database, "-cdmInfo.json")))

}

for (configPath in configs) {
  insertStudyResults(configPath)
}


