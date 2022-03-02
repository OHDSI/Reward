test_that("conf file loads", {
  config <- loadGlobalConfiguration(configPath)
  expect_s3_class(config$connectionDetails, "connectionDetails")
  validateConfigFile(configPath)
})