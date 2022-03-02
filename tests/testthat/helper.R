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

createTestReferences <- function(configPath = "tests/testthat/config/sqliteGlobalCfg.yml",
                                 vocabularyImportPath = "tests/testthat/test_vocabulary/",
                                 vocabularySchema = "main",
                                 outputFile = "reward-references.zip") {

  config <- loadGlobalConfiguration(configPath)
  unlink(config$connectionDetails$server(), force = TRUE)
  on.exit(unlink(config$connectionDetails$server(), force = TRUE))

  connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "sqlite", server = config$conectionDetails$server())
  importVocabulary(config$connectionDetails, vocabularyImportPath, vocabularySchema)
  createRewardSchema(configPath)
  connection <- DatabaseConnector::connect(config$connectionDetails)
  with_dbc_connection(connection, {
    cohortDefinition <- RJSONIO::readJSONStream(file.path("tests", "testthat", "cohorts", "atlasCohort1.json"))
    sqlDefinition <- readChar(file.path("tests", "testthat", "cohorts", "atlasCohort1.sql"),
                              file.info(file.path("tests", "testthat", "cohorts", "atlasCohort1.sql"))$size)
    insertAtlasCohortRef(connection,
                         config,
                         100,
                         webApiUrl = "test_url.com",
                         cohortDefinition = cohortDefinition,
                         sqlDefinition = sqlDefinition,
                         exposure = FALSE)

    cohortDefinition <- RJSONIO::readJSONStream(file.path("tests", "testthat", "cohorts", "atlasExposureCohort19321.json"))
    sqlDefinition <- readChar(file.path("tests", "testthat", "cohorts", "atlasExposureCohort19321.sql"),
                              file.info(file.path("tests", "testthat", "cohorts", "atlasExposureCohort19321.sql"))$size)
    insertAtlasCohortRef(connection,
                         config,
                         101,
                         webApiUrl = "test_url.com",
                         cohortDefinition = cohortDefinition,
                         sqlDefinition = sqlDefinition,
                         exposure = TRUE)

    exportReferenceTables(config, connection, exportZipFile = outputFile)
  })
}
