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

    cohortDefinition <- RJSONIO::fromJSON(readChar(file.path("cohorts", "atlasCohort1.json"),
                                                   file.info(file.path("cohorts", "atlasCohort1.json"))$size))
    sqlDefinition <- readChar(file.path("cohorts", "atlasCohort1.sql"),
                              file.info(file.path("cohorts", "atlasCohort1.sql"))$size)
    subsetOpCohortId <- insertAtlasCohortRef(connection,
                                             config,
                                             100,
                                             webApiUrl = "test_url.com",
                                             cohortDefinition = cohortDefinition,
                                             sqlDefinition = sqlDefinition,
                                             exposure = FALSE)

    cohortDefinition <- RJSONIO::fromJSON(readChar(file.path("cohorts", "atlasExposureCohort19321.json"),
                                                   file.info(file.path("cohorts", "atlasExposureCohort19321.json"))$size))
    sqlDefinition <- readChar(file.path("cohorts", "atlasExposureCohort19321.sql"),
                              file.info(file.path("cohorts", "atlasExposureCohort19321.sql"))$size)
    subsetTargetId <- insertAtlasCohortRef(connection,
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


    cohortSubsetDefinition <- CohortGenerator::createCohortSubsetDefinition(
      name = "Subset Test 1",
      definitionId = 1,
      subsetOperators = list(
        CohortGenerator::createCohortSubset(
          name = "Subset to patients in cohort 1778213",
          cohortIds = subsetOpCohortId,
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
                        subsetTargetId,
                        exposure = TRUE)

    referenceZipFile <- tempfile(fileext = ".zip")
    on.exit(unlink(referenceZipFile), add = TRUE)
    unlink(config$exportPath, recursive = TRUE, force = TRUE)
    dir.create(config$exportPath)
    exportReferenceTables(config, connection, exportZipFile = referenceZipFile)
    expect_true(file.exists(referenceZipFile))

    # Tested internally with REP, these tests are for integration
    cdmConfigPath <- "config/test.cdm.yml"
    cdmConfig <- RewardExecutionPackage::loadCdmConfiguration(cdmConfigPath)
    unlink(cdmConfig$connectionDetails$server())
    connectionDetails <- Eunomia::getEunomiaConnectionDetails(databaseFile = cdmConfig$connectionDetails$server())
    on.exit(unlink(cdmConfig$connectionDetails$server()), add = TRUE)
    on.exit(unlink(cdmConfig$referencePath), add = TRUE)
    unlink(cdmConfig$referencePath, recursive = TRUE, force = TRUE)
    RewardExecutionPackage::execute(cdmConfigPath, referenceZipFile)

    resultsZipPath <- file.path(paste0(cdmConfig$database, "RewardResults.zip"))
    unlink(config$exportPath, recursive = TRUE, force = TRUE)
    on.exit(unlink(unlink(config$exportPath), recursive = TRUE, force = TRUE), add = TRUE)
    importResults(config, resultsZipPath, connection = connection, cleanup = TRUE)


    for (table in c("scc_result", "scc_stat")) {
      sql <- "SELECT count(*) FROM @schema.@table"
      result <- DatabaseConnector::renderTranslateQuerySql(connection,
                                                           sql,
                                                           schema = config$resultsSchema,
                                                           table = table)
      expect_true(result >= 0)
    }

  })
})
