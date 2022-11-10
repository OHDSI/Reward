dbms <- getTestDbms()
if (dbms == "sqlite") {
  serverPath <- "testSqliteDb.sqlite"
  configPath <- file.path("config", "sqliteGlobalCfg.yml")
  withr::defer({
    unlink(serverPath, recursive = TRUE, force = TRUE)
  }, testthat::teardown_env())
  unlink(serverPath, recursive = TRUE, force = TRUE)
  connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "sqlite", server = serverPath)
  importVocabulary(connectionDetails, "test_vocabulary", "main")
} else {

  # location to download the JDBC drivers used in the tests
  jdbcDriverFolder <- tempfile("jdbcDrivers")
  withr::defer({
    unlink(jdbcDriverFolder, recursive = TRUE, force = TRUE)
  }, testthat::teardown_env())

  configTemplate <- yaml::read_yaml(file.path("config", paste0("global-", dbms, ".yml")))
  configPath <- tempfile(fileext = ".yml")

  configTemplate$connectionDetails <- list(
    server = Sys.getenv("CDM5_POSTGRESQL_SERVER"),
    user = Sys.getenv("CDM5_POSTGRESQL_USER"),
    password = Sys.getenv("CDM5_POSTGRESQL_PASSWORD"),
    dbms = "postgresql"
  )

  configTemplate$exportPath <- tempfile()
  configTemplate$vocabularySchema <- "mini_vocabulary"
  configTemplate$resultsSchema <- paste0("reward", gsub("[: -]", "", Sys.time(), perl = TRUE), sample(1:100, 1))
  yaml::write_yaml(configTemplate, configPath)

  pgConnection <- DatabaseConnector::connect(connectionDetails = do.call(DatabaseConnector::createConnectionDetails,
                                                                         configTemplate$connectionDetails))
  with_dbc_connection(pgConnection, {
    sql <- "DROP SCHEMA IF EXISTS @resultsDatabaseSchema CASCADE;
    CREATE SCHEMA @resultsDatabaseSchema;
    "
    DatabaseConnector::renderTranslateExecuteSql(sql = sql,
      resultsDatabaseSchema = configTemplate$resultsSchema,
      connection = pgConnection)
  })
  # Always clean up
  withr::defer(
  {
    pgConnection <- DatabaseConnector::connect(connectionDetails = do.call(DatabaseConnector::createConnectionDetails,
                                                                           configTemplate$connectionDetails))
    writeLines(paste0("Removing schema ", configTemplate$resultsSchema))
    with_dbc_connection(pgConnection, {
      sql <- "DROP SCHEMA IF EXISTS @resultsDatabaseSchema CASCADE;"
      DatabaseConnector::renderTranslateExecuteSql(
        sql = sql,
        resultsDatabaseSchema = configTemplate$resultsSchema,
        connection = pgConnection
      )
    })

    unlink(configTemplate$exportPath, recursive = TRUE, force = TRUE)
    unlink(configPath)
  }, testthat::teardown_env())

}

testDashboardConnectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "sqlite", server = "testDashboard.sqlite")
