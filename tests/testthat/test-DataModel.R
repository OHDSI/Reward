configPathClone <- file.path("config", "sqliteGlobalCfgModel.yml")

# Cache tests db to speed this up.
if (!file.exists(withTestsRoot("testSqliteDbModel.sqlite"))) {
  createModelTestDb()
}

test_that("data model loads", {

  dataModel <- RewardDataModel$new(configPathClone)
  on.exit(dataModel$finalize())
  expect_true(DBI::dbIsValid(dataModel$connection$con))
  ecs <- dataModel$getExposureCohortConceptSets()

  ecohorts <- dataModel$getExposureCohortDefinitionSet()

  ocohorts <- dataModel$getOutcomeCohortDefinitionSet()

  dataModel$finalize()
  expect_false(DBI::dbIsValid(dataModel$connection$con))
})
