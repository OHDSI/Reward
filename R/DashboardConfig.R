#' R6 for encapsulaption of dashboard configuration
#' @noRd
DashboardLoader <- R6::R6Class(
  "DashboardLoader",
  private = list(
    .validated = FALSE
  ),
  public = list(
    config = NULL,  # Holds the parsed configuration
    filePath = NULL,
    # Load YAML file
    initialize = function(filePath) {
      if (!file.exists(filePath)) {
        stop("The specified file does not exist!")
      }
      self$filePath <- filePath
      self$config <- yaml::read_yaml(filePath)
      self$validateConfig()
    },

    getShinyConfig = function() {
      targetCohortIds <- c()
      purrr::walk(self$config$cohortConceptIds, function(cohortList) {
        targetCohortIds <<- c(targetCohortIds, cohortList$cohortId)
      })

      SelfControlledCohort::createDashboardConfig(
        resultsDatabaseSchema = self$config$databaseSchema,
        vocabularyDatabaseSchema = self$config$vocabularyDatabaseSchema,
        openTargetsDatabaseSchema = self$config$openTargetsDatabaseSchema,
        dashboardName = self$config$name,
        shortName = self$config$shortName,
        dashboardType = self$config$dashboardType,
        targetCohortIds = c(self$config$targetConceptIds * 1000, targetCohortIds)
      )
    },

    getDataModel = function() {
      connectionHandler <- ResultModelManager::ConnectionHandler$new(getResultsConnectionDetails())
      SccDataModel$new(connectionHandler,
                       resultsDatabaseSettings = list(resultsDatabaseSchema = self$config$databaseSchema))
    },

    # Validate YAML configuration
    validateConfig = function() {
      requiredFields <- c("name", "databaseSchema", "description", "dashboardType",
                          "targetConceptIds")

      # Check for required fields
      for (field in requiredFields) {
        if (is.null(self$config[[field]])) {
          stop(paste("The field", field, "is missing or NULL in the configuration!"))
        }
      }

      # Validate 'cohortConceptIds' structure
      if (!is.null(self$config$cohortConceptIds)) {
        for (cohort in self$config$cohortConceptIds) {
          if (is.null(cohort$cohortId)) {
            stop("Each entry in 'cohortConceptIds' must have a 'cohortId'.")
          }

          if (is.null(cohort$conceptIds)) {
            stop("Each entry in 'cohortConceptIds' must have a 'conceptIds' list.")
          }
        }
      }

      private$.validated <- TRUE
    }
  )
)


#' List Available Dashboards
#'
#' @description
#' List available dashboards in reward config context
#'
#' @export
listAvailableDashboards <- function() {
  cli::cli_h1("Available dashboards")
  for (filef in list.files('dashboards', pattern = "*.yml"))
    cli::cli_bullets(c("*" = filef))
}

#' Get dashboard configuration
#' @export
#' @param dashboardName config file name
getDashboardConfig <- function(dashboardName) {
  path <- file.path("dashboards", paste0(dashboardName, ".yml"))
  cli::cli_alert_info("loading {path}")
  return(DashboardLoader$new(path))
}