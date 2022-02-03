test_that("Build database schema, add test cohorts", {
  createRewardSchema(configPath)
  config <- loadGlobalConfiguration(configPath)
  connection <- DatabaseConnector::connect(config$connectionDetails)

  # Check tables exist
  with_dbc_connection(connection, {
    for (table in config$referenceTables) {
      sql <- "SELECT count(*) FROM @schema.@table"
      result <- DatabaseConnector::renderTranslateQuerySql(connection,
                                                           sql,
                                                           schema = config$resultsSchema,
                                                           table = table)
      expect_true(result >= 0)
    }

    cohortDefinition <- RJSONIO::readJSONStream(file.path("cohorts", "atlasCohort1.json"))
    sqlDefinition <- readChar(file.path("cohorts", "atlasCohort1.sql"),
                              file.info(file.path("cohorts", "atlasCohort1.sql"))$size)
    insertAtlasCohortRef(connection,
                         config,
                         100,
                         webApiUrl = "test_url.com",
                         cohortDefinition = cohortDefinition,
                         sqlDefinition = sqlDefinition,
                         exposure = FALSE)

    cohortDefinition <- RJSONIO::readJSONStream(file.path("cohorts", "atlasExposureCohort19321.json"))
    sqlDefinition <- readChar(file.path("cohorts", "atlasExposureCohort19321.sql"),
                              file.info(file.path("cohorts", "atlasExposureCohort19321.sql"))$size)
    insertAtlasCohortRef(connection,
                         config,
                         101,
                         webApiUrl = "test_url.com",
                         cohortDefinition = cohortDefinition,
                         sqlDefinition = sqlDefinition,
                         exposure = TRUE)
    expect_error({
      insertAtlasCohortRef(connection,
                           config,
                           101,
                           webApiUrl = "test_url.com",
                           cohortDefinition = cohortDefinition,
                           sqlDefinition = sqlDefinition,
                           exposure = TRUE)
    }, regexp = "Cohort 101 already in database, use removeAtlasCohort to clear entry references")

    exportReferenceTables(config, connection)
    expect_true(file.exists("reward-references.zip"))
  })
})
