#' DashboardLoader R6 Class
#'
#' Encapsulates loading and validation of dashboard configuration from a YAML file, and provides accessor methods for configuration and associated data structures.
#'
#' @description
#' The `DashboardLoader` class is responsible for reading a YAML dashboard configuration, validating its structure, and providing helper methods for generating dashboard-related configurations and data models.
#'
#' @details
#' The class ensures the YAML config file contains all required fields, and that list structures expected by downstream analyses are present. Validation is performed at construction; errors in the YAML file halt further execution.
#'
#' The loader supports generating dashboard and data model objects from parsed configuration, streamlining the process of dashboard setup and ensuring reproducibility.
#' @export
#' @examples
#' \dontrun{
#' # Load dashboard configuration
#' loader <- DashboardLoader$new("path/to/dashboard.yaml")
#'
#' # Get configuration for Shiny dashboard
#' shinyConfig <- loader$getShinyConfig()
#'
#' # Get results data model
#' dataModel <- loader$getDataModel()
#' }
#'
#' @field config A list containing the parsed dashboard configuration, read from a YAML file. All dashboard setup and parameters are accessed via this field.
#' @field filePath Character. Stores the path to the YAML configuration file used for loading the dashboard setup.
DashboardLoader <- R6::R6Class(
  "DashboardLoader",
  private = list(
    .validated = FALSE
  ),
  public = list(
    config = NULL,  # Holds the parsed configuration
    filePath = NULL,
    #' Load and Validate dashboard config
    #' @description
    #' Constructor. Loads and validates the dashboard configuration at the given `filePath`. Throws an error if the file does not exist or fails validation.
    #' @param filePath path to yaml file
    initialize = function(filePath) {
      if (!file.exists(filePath)) {
        stop("The specified file does not exist!")
      }
      self$filePath <- filePath
      self$config <- yaml::read_yaml(filePath)
      self$validateConfig()
    },
    #' Get shiny config
    #' @description
    #' Generates and returns a dashboard configuration object suitable for use in Shiny applications, built from the loaded configuration. Aggregates all cohort and target concept IDs.
    getShinyConfig = function() {
      targetCohortIds <- c()
      purrr::walk(self$config$cohortConceptIds, function(cohortList) {
        targetCohortIds <<- c(targetCohortIds, cohortList$cohortId)
      })

      createDashboardConfig(
        resultsDatabaseSchema = self$config$databaseSchema,
        vocabularyDatabaseSchema = self$config$vocabularyDatabaseSchema,
        openTargetsDatabaseSchema = self$config$openTargetsDatabaseSchema,
        dashboardName = self$config$name,
        shortName = self$config$shortName,
        dashboardType = self$config$dashboardType,
        targetCohortIds = c(self$config$targetConceptIds * 1000, targetCohortIds)
      )
    },
    #' get data model
    #' @description
    #' Instantiates and returns a data model object (`SccDataModel`) for analysis, based on the dashboard configuration and an open database connection.
    getDataModel = function() {
      connectionHandler <- ResultModelManager::ConnectionHandler$new(getResultsConnectionDetails())
      SccDataModel$new(connectionHandler,
                       resultsDatabaseSettings = list(resultsDatabaseSchema = self$config$databaseSchema))
    },

    #' Validate config
    #' @description
    #' Checks the loaded configuration for required fields (`name`, `databaseSchema`, `description`, `dashboardType`, `targetConceptIds`). Also checks each entry in `cohortConceptIds` for both `cohortId` and `conceptIds`. Throws an error if validation fails.
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