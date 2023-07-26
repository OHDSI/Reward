###
# Template app for deployment to posit connect platform
###
library(Reward)

driverPath <- Sys.getenv("DATABASECONNECTOR_JAR_FOLDER", unset = "./")

if (!any(grepl("postgresql", list.files(driverPath, pattern = ".jar")))) {
  DatabaseConnector::downloadJdbcDrivers("postgresql", pathToDriver = driverPath)
}

launchDashboard(configPath = "reward-cfg.yml",  dashboardConfigPath = "dashboard-config.yml")
