configPathClone <- file.path("config", "sqliteGlobalCfgModel.yml")

# Cache tests db to speed this up.
if (!file.exists(withTestsRoot("testSqliteDbModel.sqlite"))) {
  createModelTestDb()
}

test_that("Negative outcome controls works", {
  outcomeControls <- mapNegativeControlOutcomes(configPathClone)
  checkmate::expect_data_frame(outcomeControls)
})
