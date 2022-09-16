configPathClone <- file.path("config", "sqliteGlobalCfgModel.yml")

# Cache tests db to speed this up.
if (!file.exists(withTestsRoot("testSqliteDbModel.sqlite"))) {
  createModelTestDb()
}

test_that("data model loads", {

  dataModel <- RewardDataModel$new(configPathClone)
  on.exit(dataModel$finalize())
  ecs <- dataModel$getExposureCohortConceptSets()

  ecohorts <- dataModel$getExposureCohortDefinitionSet()

  ocohorts <- dataModel$getOutcomeCohortDefinitionSet()

  dataModel$finalize()
})
