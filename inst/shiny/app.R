library(shinydashboard)
library(shinycssloaders)
library(shinyWidgets)
library(DT)
library(reactable)
library(plotly)
library(ggplot2)

Sys.setenv("DATABASECONNECTOR_JAR_FOLDER" = "./")

connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "postgresql",
  server = Sys.getenv('REWARD_DB_SERVER'),
  user = Sys.getenv('REWARD_DB_USER'),
  password = Sys.getenv('REWARD_DB_PASSWORD'),
  pathToDriver = "./"
)

config <- yaml::read_yaml('config.yml')

targetCohortIds <- c()
purrr::walk(config$cohortConceptIds, function(cohortList) {
  targetCohortIds <<- c(targetCohortIds, cohortList$cohortId)
})


Reward::launchDashboard(
  connectionDetails,
  dashboardConfig = list(
    resultsDatabaseSchema = config$databaseSchema,
    dashboardName = config$name,
    shortName = config$shortName,
    openTargetsDatabaseSchema = config$openTargetsDatabaseSchema,
    dashboardType = config$dashboardType,
    targetCohortIds = c(config$targetConceptIds * 1000, targetCohortIds)
  )
)
