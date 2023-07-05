getTestDbms <- function() {
  getOption("dbms", default = "sqlite")
}

# When devtools::load_all is run, create symbolic link for sql directory
# Allows testing with devtools::test on osx and linux
if (Sys.getenv("DEVTOOLS_LOAD") == "true" & .Platform$OS.type == "unix") {
  print("setting sql folder symobolic link")
  packageRoot <- normalizePath(system.file("..", package = "Reward"))
  # Create symbolic link so code can be used in devtools::test()
  linkPath <- file.path(packageRoot, "sql")
  if (!file.exists(linkPath)) {
    R.utils::createLink(link = linkPath, system.file("sql", package = "Reward"))
    options("use.devtools.sql_shim" = TRUE)
    withr::defer(unlink(linkPath), testthat::teardown_env())
  }
}

#' This function is only really used for testing
#' A full vocabulary should have indexes and constraints etc
#' The large concept_ancestor and concept tables will likely be difficult to import
importVocabulary <- function(connectionDetails, vocabularyImportPath, vocabularySchema) {
  vocabularyFiles <- c("CONCEPT.csv",
                       "CONCEPT_ANCESTOR.csv",
                       "CONCEPT_CLASS.csv",
                       "CONCEPT_RELATIONSHIP.csv",
                       "CONCEPT_SYNONYM.csv",
                       "DOMAIN.csv",
                       "DRUG_STRENGTH.csv",
                       "METADATA.csv",
                       "RELATIONSHIP.csv",
                       "SOURCE_TO_CONCEPT_MAP.csv",
                       "VOCABULARY.csv")

  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  for (fileName in vocabularyFiles) {
    message("Inserting ", fileName)
    file <- file.path(vocabularyImportPath, fileName)
    data <- vroom::vroom(file, delim = ",", show_col_types = FALSE)
    tableName <- gsub(".csv", "", fileName)
    suppressWarnings(
      # Throws warning for DOMAIN - reserved SQL keyword
      DatabaseConnector::insertTable(connection,
                                     tableName = tableName,
                                     data = data,
                                     databaseSchema = vocabularySchema,
                                     dropTableIfExists = TRUE,
                                     createTable = TRUE,
                                     progressBar = TRUE)
    )

  }
}

#' utility function to make sure connection is closed after usage
with_dbc_connection <- function(connection, code) {
  on.exit({
    DatabaseConnector::disconnect(connection)
  })
  eval(substitute(code), envir = connection, enclos = parent.frame())
}

# Consistent path for config files if using devtools::load_all() or inside tests
withTestsRoot <- function(...) {
 isTestEnv <- isTRUE(grep("testthat", getwd()) > 0)
 if (isTestEnv) {
   return(file.path(normalizePath("."), ...))
 }
 return(normalizePath(file.path("tests", "testthat", ...)))
}

createTestReferences <- function(configPath = file.path(withTestsRoot(), "config", "sqliteGlobalCfgModel.yml"),
                                 vocabularyImportPath = file.path(withTestsRoot(), "test_vocabulary"),
                                 vocabularySchema = "main",
                                 analysisSettingsFilePath = file.path(withTestsRoot(), "config", "testSccArgs.json"),
                                 outputFile = "reward-test-references.zip",
                                 deleteDb = TRUE) {
  config <- loadGlobalConfiguration(configPath)
  unlink(config$connectionDetails$server(), force = TRUE)
  if (deleteDb) {
    on.exit(unlink(config$connectionDetails$server(), force = TRUE))
  }
  connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "sqlite", server = config$conectionDetails$server())
  importVocabulary(config$connectionDetails, vocabularyImportPath, vocabularySchema)
  createRewardSchema(configPath, settingsFilePath = analysisSettingsFilePath)
  connection <- DatabaseConnector::connect(config$connectionDetails)
  with_dbc_connection(connection, {
    cohortDefinition <- RJSONIO::readJSONStream(withTestsRoot("cohorts", "atlasCohort1.json"))
    sqlDefinition <- readChar(withTestsRoot("cohorts", "atlasCohort1.sql"),
                              file.info(withTestsRoot("cohorts", "atlasCohort1.sql"))$size)
    toId <- insertAtlasCohortRef(connection,
                         config,
                         100,
                         webApiUrl = "test_url.com",
                         cohortDefinition = cohortDefinition,
                         sqlDefinition = sqlDefinition,
                         exposure = FALSE)

    cohortDefinition <- RJSONIO::readJSONStream(withTestsRoot("cohorts", "atlasExposureCohort19321.json"))
    sqlDefinition <- readChar(withTestsRoot("cohorts", "atlasExposureCohort19321.sql"),
                              file.info(withTestsRoot("cohorts", "atlasExposureCohort19321.sql"))$size)
    insertAtlasCohortRef(connection,
                         config,
                         101,
                         webApiUrl = "test_url.com",
                         cohortDefinition = cohortDefinition,
                         sqlDefinition = sqlDefinition,
                         exposure = TRUE)

    # Add test subset definition that subsets an exposure by an outcome
    cohortSubsetDefinition <- CohortGenerator::createCohortSubsetDefinition(
      name = "Subset Test 1",
      definitionId = 1,
      subsetOperators = list(
        CohortGenerator::createCohortSubset(
          name = "Test subset op",
          cohortIds = toId,
          cohortCombinationOperator = "any",
          negate = FALSE,
          startWindow = CohortGenerator::createSubsetCohortWindow(
            startDay = -9999,
            endDay = 0,
            targetAnchor = "cohortStart"
          ),
          endWindow = CohortGenerator::createSubsetCohortWindow(
            startDay = 0,
            endDay = 9999,
            targetAnchor = "cohortStart"
          )
        )
      )
    )

    addSubsetDefinition(connection,
                        config,
                        cohortSubsetDefinition,
                        1310149000, # Warfrin
                        exposure = TRUE)

    exportReferenceTables(config, connection, exportZipFile = outputFile)
  })
}


createModelTestDb <- function(outputPath = withTestsRoot("testSqliteDbModel.sqlite")) {
  configPath <- withTestsRoot("config", "sqliteGlobalCfgModel.yml")
  config <- loadGlobalConfiguration(configPath)
  createTestReferences(outputFile = "reward-test-references.zip", deleteDb = FALSE)
  on.exit(unlink("reward-test-references.zip"), add = TRUE)

  # Tested internally with REP, these tests are for integration
  cdmConfigPath <- withTestsRoot("config", "test.cdm.yml")
  cdmConfig <- RewardExecutionPackage::loadCdmConfiguration(cdmConfigPath)
  connectionDetails <- Eunomia::getEunomiaConnectionDetails(databaseFile = cdmConfig$connectionDetails$server())
  on.exit(unlink(cdmConfig$connectionDetails$server()), add = TRUE)
  on.exit(unlink(unlink(config$exportPath), recursive = TRUE, force = TRUE), add = TRUE)

  RewardExecutionPackage::execute(cdmConfigPath, "reward-test-references.zip")
  resultsZipPath <- file.path(cdmConfig$exportPath, paste0("reward-results-", cdmConfig$database, ".zip"))
  importResults(config, resultsZipPath, cleanup = TRUE)
  file.copy(config$connectionDetails$server(), outputPath, overwrite = TRUE)
}
