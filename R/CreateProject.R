#' Create Project
#' @description
#'
#' Create a new project.
#'
#' This function will do the following:
#'
#' * Create a new project in an empty or existing directory.
#' * Create a new config.yml file
#' * Add default all-by-all cohort templates
#' * Create csv files for configuration of target cohort
#' @param projectDir            directory to place configuration
#' @param overwite              overwrite existing configuration files
createProject <- function(projectDir = getwd(), overwite = FALSE) {
  if (!dir.exists(projectDir)) {
    dir.create(projectDir, recursive = TRUE)
  }

  if (projectDir != getwd()) {
    setwd(projectDir)
  }

  configPath <- file.path(projectDir, "config.yml")
  if (file.exists(configPath) & !overwite) {
    cli::cli_abort("config.yml already exists. Quitting")
  }

  templateFile <- system.file("yml", "configTemplate.yml", package = utils::packageName())
  file.copy(templateFile, configPath)
  cli::cli_alert_success("Default config file {configPath} created. You will need to modify this to match your settings")

  dir.create(file.path(projectDir, "cohorts"))

  writeLines("cohortId,isExposure", file.path(projectDir, "cohorts", "rewardAtlasCohorts.csv"))
  writeLines("cohortId", file.path(projectDir, "cohorts", "phenotypeLibraryOutcomesIndications.csv"))
  writeLines("cohortId", file.path(projectDir, "cohorts", "phenotypeLibraryExposures.csv"))

  cli::cli_alert_success("Created empty atlas cohort definitions")

  dir.create(file.path("analysis", "tasks"), showWarnings = FALSE, recursive = TRUE)

  cohortsFile <- system.file("scripts", "1-CreateCohorts.R", package = utils::packageName())
  file.copy(cohortsFile, file.path("analysis", "tasks", "1-CreateCohorts.R"))

  cli::cli_alert_success("Base project created. Next, configure your results and cdm databases")
}