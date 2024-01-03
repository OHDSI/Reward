# Copyright 2022 Observational Health Data Sciences and Informatics
#
# This file is part of Reward
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

configCreatorUi <- function(id) {
  ns <- shiny::NS(id)
  ui <- shiny::tagList(
    shiny::titlePanel("Reward Dashboard Config Creator"),
    shiny::p("This tool is designed as a utility for creating Reward dashboard configurations.
    To use, select the relevant options here and save a configuratuion file.
    This file can then be used to create a new dashboard sqlite file, or postgresql schema containing empirically
    calibrated effect estimates.
    "),
    shiny::fluidRow(
      shiny::column(
        width = 6,
        shiny::textInput(ns("name"), label = "Dashboard Name", width = "100%")
      ),
      shiny::column(
        width = 6,
        shiny::textInput(ns("shortName"), label = "Short Name", width = "100%")
      )
    ),
    shiny::fluidRow(
      shiny::column(
        width = 12,
        shiny::textAreaInput(ns("description"), label = "Description", resize = "none", width = '100%')
      )
    ),
    shiny::fluidRow(
      shiny::column(
        width = 4,
        shiny::radioButtons(
          inputId = ns("exposureOutcome"),
          label = "Exposure or Outcome based",
          choices = c("Exposures", "Outcomes"),
          selected = "Exposures")
      ),
    ),

    shiny::fluidRow(
      shiny::column(
        width = 12,
        shiny::h4("Select Data Sources"),
        shinycssloaders::withSpinner(reactable::reactableOutput(ns("dataSourceSelector")))
      )
    ),
    shiny::fluidRow(
      shiny::column(
        width = 12,
        shiny::h4("Select Analysis Types"),
        shinycssloaders::withSpinner(reactable::reactableOutput(ns("analysisTypeSelector")))
      )
    ),
    shiny::fluidRow(
      shiny::column(
        width = 12,
        shiny::h4("Select Cohorts"),
        shiny::p("Select up to 15:"),
        shinycssloaders::withSpinner(reactable::reactableOutput(ns("cohortSelectorTable")))
      )
    ),
    shiny::fluidRow(
      shiny::column(
        width = 3,
        shiny::actionButton(ns("previewConfig"), "Preview configuration")
      )
    ),
    shiny::fluidRow(
      shiny::column(
        width = 3,
        shiny::p()
      )
    )
  )
  return(ui)
}

configModule <- function(id, model) {
  ns <- shiny::NS(id)
  shiny::moduleServer(id, function(input, output, session) {
    shiny::shinyOptions(cache = cachem::cache_disk("~/.rewardUiCache"))

    output$analysisTypeSelector <- reactable::renderReactable({
      data <- model$getAnalysisSettings(FALSE) %>% dplyr::select(-options)
      colnames(data) <- SqlRender::camelCaseToTitleCase(colnames(data))
      reactable::reactable(data, selection = "multiple")
    })

    selectedAnalysisSettings <- shiny::reactive({
      tblState <- reactable::getReactableState("analysisTypeSelector")
      data <- model$getAnalysisSettings(FALSE)[tblState$selected,]
      return(as.integer(data$analysisId))
    })

    output$dataSourceSelector <- reactable::renderReactable({
      data <- model$getDataSources()
      colnames(data) <- SqlRender::camelCaseToTitleCase(colnames(data))
      reactable::reactable(data, selection = "multiple")
    })

    selectedDataSources <- shiny::reactive({
      tblState <- reactable::getReactableState("dataSourceSelector")
      data <- model$getDataSources()[tblState$selected,]
      return(as.integer(data$sourceId))
    })

    getCohortSet <- shiny::reactive({
      if (input$exposureOutcome == "Outcomes") {
        cohortSet <- model$getOutcomeCohortDefinitionSet()
      } else if (input$exposureOutcome == "Exposures") {
        cohortSet <- model$getExposureCohortDefinitionSet()
      } else {
        cohortSet <- data.frame()
      }
      return(cohortSet)
    }) %>% shiny::bindCache(input$exposureOutcome)


    output$cohortSelectorTable <- reactable::renderReactable({
      shiny::withProgress({
        data <- getCohortSet() %>% dplyr::select(cohortDefinitionId, cohortDefinitionName)

        colnames(data) <- SqlRender::camelCaseToTitleCase(colnames(data))
        table <- reactable::reactable(data = data,
                                      columns = list(
                                        "Cohort Definition Id" = reactable::colDef(
                                          maxWidth = 200
                                        )
                                      ),
                                      searchable = TRUE,
                                      resizable = TRUE,
                                      selection = "multiple")
      }, message = "Loading cohort set, may take some time")

      return(table)
    })

    selectedCohortIds <- shiny::reactive({
      tblState <- reactable::getReactableState("cohortSelectorTable")
      data <- getCohortSet()[tblState$selected[1:min(15, length(tblState$selected))],]
      return(as.character(bit64::as.integer64(data$cohortDefinitionId)))
    })

    configSettings <- shiny::reactive(
      list(
        dashboardName = input$name,
        shortName = input$shortName,
        description = input$description,
        exposureDashboard = input$exposureOutcome == "Exposures",
        analysisSettings = selectedAnalysisSettings(),
        dataSources = selectedDataSources(),
        cohortIds = selectedCohortIds()
      )
    )

    output$saveConfig <- shiny::downloadHandler(
      filename = "dashboardConfig.yml",
      content = function(file) {
        # TODO: validation
        shiny::removeModal()
        writeLines(yaml::as.yaml(configSettings()), con = file)
      },
      contentType = "application/yaml"
    )

    shiny::observeEvent(input$previewConfig, {
      yamlConfig <- yaml::as.yaml(configSettings())

      shiny::showModal(
        shiny::modalDialog(
          title = NULL,
          shiny::tagList(
            shinyAce::aceEditor("yamlConfig",
                                value = yamlConfig,
                                mode = "yaml",
                                readOnly = TRUE),
            shiny::p(shiny::textOutput("configSaveError"), style = "color:red;")
          ),
          easyClose = TRUE,
          footer = shiny::fluidRow(
            shiny::column(width = 2, shiny::downloadButton(ns("saveConfig"), "Save")),
            shiny::column(width = 1, shiny::actionButton(ns("closeModal"), "Close")),
          )
        )
      )
    })

    shiny::observeEvent(input$closeModal, shiny::removeModal())
  })
}


configDashboardUi <- function() {
  shinydashboard::dashboardPage(
    header = shinydashboard::dashboardHeader(title = "Reward Config Creator"),
    sidebar = shinydashboard::dashboardSidebar(
      shinydashboard::sidebarMenu(
        shinydashboard::menuItem(
          "Calibration Explorer",
          tabName = "calibrationExplorer",
          icon = icon("gears", verify_fa = FALSE)
        ),
        shinydashboard::menuItem(
          "Create Config",
          tabName = "configCreator",
          icon = icon("dashboard", verify_fa = FALSE)
        ),
        shinydashboard::menuItem(
          "Create Cohort Method study",
          tabName = "cmStudyCreator",
          icon = icon("users", verify_fa = FALSE)
        )
      )
    ),
    body = shinydashboard::dashboardBody(
      shinydashboard::tabItems(
        shinydashboard::tabItem(
          "configCreator",
          shinydashboard::box(width = NULL, configCreatorUi("config"))
        ),
        shinydashboard::tabItem(
          "calibrationExplorer",
          shinydashboard::box(width = NULL, calibrationExplorerUi("calibrationExplorer"))
        ),
        shinydashboard::tabItem(
          "cmStudyCreator",
          cmStudyUi()
        )
      )
    )
  )
}

configDashboardServer <- function(input, output, session) {
  configModule(id = "config", model)
  calibrationExplorerModule(id = "calibrationExplorer", model)
  cmStudyModule(model = model)
}

#' Shiny app to create stand-alone dashboaard database from reward data
#' @details
#' Creates a configuration file for a Reward dashboard Shiny application to allow investigation in to
#' a subset of exposures or outcomes
#'
#' @param configPath                Path to Reward global configuration file
#' @inheritParams                   loadGlobalConfiguration
#' @export
launchDashboardCreator <- function(configPath,
                                   usePooledConnection = TRUE) {
  checkmate::assertFileExists(configPath)
  .GlobalEnv$model <- RewardDataModel$new(configPath, usePooledConnection = usePooledConnection)
  shiny::shinyApp(server = configDashboardServer, ui = configDashboardUi, onStart = function() {
    shiny::onStop(function() {
      writeLines("Closing connections")
      model$finalize()
    })
  })
}