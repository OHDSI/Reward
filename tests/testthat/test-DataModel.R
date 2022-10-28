configPathClone <- file.path("config", "sqliteGlobalCfgModel.yml")

# Cache tests db to speed this up.
if (!file.exists(withTestsRoot("testSqliteDbModel.sqlite"))) {
  createModelTestDb()
}

test_that("data model loads", {

  dataModel <- RewardDataModel$new(configPathClone)
  on.exit(dataModel$finalize())
  ecs <- dataModel$getExposureCohortConceptSets()

  eCohorts <- dataModel$getExposureCohortDefinitionSet()
  checkmate::expect_data_frame(eCohorts)
  oCohorts <- dataModel$getOutcomeCohortDefinitionSet()
  checkmate::expect_data_frame(oCohorts)

  dashDataModel <- DashboardDataModel$new("config/testDashboard.yml",
                                          connectionDetails = testDashboardConnectionDetails,
                                          cemConnectionDetails = dataModel$config$cemConnectionDetails,
                                          resultDatabaseSchema = "main")

  cemConnection <- dashDataModel$getCemConnection()
  checkmate::expect_class(cemConnection, "CemDatabaseBackend")
  cemConnection$finalize()
  on.exit(dashDataModel$finalize())
  ecs <- dashDataModel$getExposureCohortConceptSets()

  eCohorts <- dashDataModel$getExposureCohortDefinitionSet()
  checkmate::expect_data_frame(eCohorts)
  oCohorts <- dashDataModel$getOutcomeCohortDefinitionSet()
  checkmate::expect_data_frame(oCohorts)
  # checkmate::expect_data_frame(dataModel$getCohort(1))
  checkmate::expect_data_frame(dataModel$getAnalysisSettings())
  # checkmate::expect_data_frame(dataModel$getCohortStats(1, FALSE))
  # checkmate::expect_data_frame(dataModel$getCohortStats(1, FALSE))

  ds <- dashDataModel$getDataSources()
  checkmate::expect_data_frame(ds)

  checkmate::expect_data_frame(dashDataModel$getOutcomeCohortConceptSets())
  dashDataModel$finalize()
})
