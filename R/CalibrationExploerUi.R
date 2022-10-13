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

calibrationExplorerUi <- function(id) {
  ns <- shiny::NS(id)
  ui <- shiny::fluidPage(
    shiny::titlePanel("Reward Calibration Explorer"),
    shiny::fluidRow(
      shiny::column(
        width = 6,
        shiny::selectInput(
          inputId = ns("exposureOutcome"),
          label = "Exposure/Outcome",
          width = "100%",
          choices = c("Exposures", "Outcomes"),
          selected = NULL)
      ),
      shiny::column(
        width = 6,
        shiny::selectizeInput(ns("cohortSelection"),
                              label = "Cohorts:",
                              width = "100%",
                              choices = NULL,
                              multiple = FALSE)
      ),
    ),
    shiny::fluidRow(
      shiny::column(
        width = 12,
        shiny::h3(shiny::textOutput(ns("cohortName"))),

        shiny::uiOutput(ns("tabArea"))
      )
    )
  )
  return(ui)
}

calibrationExplorerModule <- function(id, model) {
  ns <- shiny::NS(id)
  shiny::moduleServer(id, function(input, output, session) {
    shiny::shinyOptions(cache = cachem::cache_disk("~/.rewardUiCache"))
    cemConnection <- model$getCemConnection()

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

    getCohortDropDownSelection <- shiny::reactive({
      cohortSet <- getCohortSet()
      return(cohortSet$shortName)
    })

    shiny::observe({
      analysisSettingChoices <- model$getAnalysisSettings() %>%
        dplyr::select(analysisName,
                      analysisId)

      analysisSetting <- analysisSettingChoices$analysisId
      names(analysisSetting) <- analysisSettingChoices$analysisName
      shiny::updateSelectInput(session = session,
                               inputId = "selectedAnalysisType",
                               choices = analysisSetting)
    })

    shiny::observeEvent(eventExpr = input$exposureOutcome, handlerExpr = {
      shiny::updateSelectizeInput(session, "cohortSelection", choices = getCohortDropDownSelection(), server = TRUE)
    })

    getSelectedCohortData <- shiny::reactive({
      cohortSet <- getCohortSet()
      selectedName <- input$cohortSelection
      cohortDefinitionId <- cohortSet %>%
        dplyr::filter(shortName == selectedName) %>%
        dplyr::select(cohortDefinitionId) %>%
        dplyr::pull()

      if (length(cohortDefinitionId))
        return(model$getCohort(cohortDefinitionId))

      return(NULL)
    })

    getSelectedOutcomeType <- shiny::reactive({
      if (!is.null(input$selectedOutcomeType))
        return(as.integer(input$selectedOutcomeType))

      return(NULL)
    })

    selectedCohort <- shiny::reactive({
      cohort <- getSelectedCohortData()

      if (!is.null(cohort)) {
        cohort$isExposure <- input$exposureOutcome == "Exposures"
        cohort$selectedOutcomeType <- getSelectedOutcomeType()
        cohort$analysisId <- input$selectedAnalysisType
        return(cohort)
      }

      list(cohortDefinitionId = -1,
           shortName = "No Cohort Selected",
           conceptSet = list(),
           isExposure = TRUE,
           analysisSetting = 1)
    })

    output$cohortName <- shiny::renderText({
      cohort <- selectedCohort()
      cohort$shortName
    })

    getCohortStats <- shiny::reactive({
      cohort <- selectedCohort()
      data <- model$getCohortStats(cohort$cohortDefinitionId, cohort$isExposure)
      colnames(data) <- SqlRender::camelCaseToTitleCase(colnames(data))
      reactable::reactable(data = data)
    })

    output$cohortInfoTable <- reactable::renderReactable({
      getCohortStats()
    })


    if (!is.null(cemConnection)) {
      CemConnector::negativeControlSelectorModule("negativeControls",
                                                  backend = cemConnection,
                                                  conceptInput = shiny::reactive({
                                                    selectedCohort()$conceptSet
                                                  }),
                                                  nControls = shiny::reactive({
                                                    5000
                                                  }),
                                                  isOutcomeSearch = shiny::reactive({
                                                    cohort <- selectedCohort()
                                                    cohort$isExposure
                                                  }))

      calibrationPlotServer("calibrationPlot", model, selectedCohort)

      output$tabArea <- shiny::renderUI({
        shiny::tabsetPanel(
          id = "mainTabs",
          type = "pills",
          shiny::tabPanel(
            "Cohort Statistics",
            shiny::p("Available record counts for selected cohort are caluclated as non-zero uncalibrated effect estimates
            observed in data sources for selected cohorts"),
            reactable::reactableOutput(ns("cohortInfoTable"))
          ),
          shiny::tabPanel(
            "Negative Control Concepts",
            shiny::p("Negative control concepts automatically selected from common evidence model"),
            CemConnector::negativeControlSelectorUi(ns("negativeControls"))
          ),
          shiny::tabPanel(
            title = "Calibration",
            shiny::conditionalPanel(
              condition = "input.exposureOutcome == 'Exposures'",
              shiny::selectInput(ns("selectedOutcomeType"),
                                 "Outcome control cohort type",
                                 choices = c("One Diagnosis Code" = 2,
                                             "2 Diagnosis Codes" = 0,
                                             "Inpatient Visit" = 1))
            ),
            shiny::selectInput(
              inputId = ns("selectedAnalysisType"),
              label = "SCC analysis settings",
              choices = NULL),
            calibrationPlotUi(ns("calibrationPlot")))
        )
      })
    } else {
      output$tabArea <- shiny::renderUI({
        shiny::tabsetPanel(
          id = ns("mainTabs"),
          type = "pills",
          shiny::tabPanel(
            "Cohort Statistics",
            shiny::p("Available record counts for selected cohort are caluclated as non-zero uncalibrated effect estimates
            observed in data sources for selected cohorts"),
            reactable::reactableOutput(ns("cohortInfoTable")),
            shiny::p("Note unable to get negative controls as - CEM connection service is unavailable.
           Please contact system administrator for resolution.", style = "color:red;")
          )
        )
      })
    }
  })
}