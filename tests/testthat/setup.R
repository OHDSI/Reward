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
}

testDashboardConnectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "sqlite", server = "testDashboard.sqlite")
