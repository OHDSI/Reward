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

################################
# Launch CemConnector test API #
################################
cemApiUrl <- getOption("CemConnector.useHostedUrl", default = FALSE)
cemConnectionDetails <- getOption("CemConnectionDetails", default = NULL)
cemTestSchema <- getOption("cemTestSchema", default = "main")
cemVocabularySchema <- getOption("cemVocabularySchema", default = "main")
cemSourceInfoSchema <- getOption("cemSourceInfoSchema", default = "main")

if (is.null(cemApiUrl) | !("connectionDetails" %in% class(cemConnectionDetails))) {
  # Load API in separate process
  serverStart <- function(pipe, apiPort, cemSchema, vocabularySchema, sourceSchema, ...) {
    library(CemConnector) # Required for separate process
    connectionDetails <- DatabaseConnector::createConnectionDetails(...)

    tryCatch(
    {
      api <- CemConnector::loadApi(connectionDetails,
                                   cemSchema = cemSchema,
                                   vocabularySchema = vocabularySchema,
                                   sourceSchema = sourceSchema)
      api$setDocs(FALSE)
      writeLines("API LOADED", con = pipe)
      api$run(port = apiPort)
    },
      error = function(err) {
        writeLines("API FAILED", con = pipe)
        writeLines(err, con = pipe)
      }
    )
  }

  sqlidb <- tempfile(fileext = ".sqlite")
  cemConnectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "sqlite", server = sqlidb)
  .loadCemTestFixtures(cemConnectionDetails)

  withr::defer(
  {
    unlink(sqlidb)
  },
    testthat::teardown_env()
  )

  apiPort <- httpuv::randomPort(8000, 8080)
  cemApiUrl <- paste0("http://localhost:", apiPort)

  sessionCommunication <- tempfile()
  writeLines("", con = sessionCommunication)
  print("Starting api session...")

  stdOut <- tempfile()
  errorOut <- tempfile()

  apiSession <- callr::r_bg(serverStart,
                            stdout = stdOut,
                            stderr = errorOut,
                            package = TRUE,
                            args = list(
                              pipe = sessionCommunication,
                              apiPort = apiPort,
                              dbms = "sqlite",
                              server = sqlidb,
                              cemSchema = cemTestSchema,
                              vocabularySchema = cemVocabularySchema,
                              sourceSchema = cemSourceInfoSchema
                            ))

  withr::defer(
  {
    apiSession$kill()
    unlink(sessionCommunication)
    unlink(stdOut)
    unlink(errorOut)
  },
    testthat::teardown_env()
  )


  apiSessionReady <- function() {
    if (apiSession$is_alive()) {
      input <- readLines(sessionCommunication)
      failed <- any(grep("API FAILED", input) == 1)
      loaded <- any(grep("API LOADED", input) == 1)

      if (failed) {
        errorLines <- readLines(errorOut)
        studLines <- readLines(stdOut)
        stop("Failed to load API. Error in configuration?\n", errorLines, studLines)
      }

      return(loaded)
    }
    # If the session is dead, stop
    errorLines <- readLines(errorOut)
    studLines <- readLines(stdOut)
    stop(paste("Api session failed to start\n", errorLines, studLines))
  }

  tryCatch(
  {
    # poll status until failure or load
    while (!apiSessionReady()) {
      Sys.sleep(0.01) # Allow time for process to start, needs to connect to database...
    }
    useTestPlumber <- TRUE
    print("Session started")
  },
    error = function(err) {
      message("Failed to load API will skip web request tests")
      print(err)
    }
  )
} else {
  useTestPlumber <- TRUE
  message(paste("Using live web backend at", cemApiUrl))
}