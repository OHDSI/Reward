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

#' UI for calibarion plot - uses namespacing
calibrationPlotUi <- function(id,
                              figureTitle = "Figure 1.",
                              figureText = "Plot of calibration of effect estimates. Blue dots are negative control effect estimates.") {
  ns <- shiny::NS(id)
  shiny::tagList(
    shiny::actionButton(inputId = ns("getNullDist"), label = "Get negative control data"),
    shiny::conditionalPanel(
      ns = ns,
      condition = "input.getNullDist > 0",
      shinycssloaders::withSpinner(plotly::plotlyOutput(ns("calibrationPlot"), height = 500)),
      shiny::div(
        shiny::strong(figureTitle),
        shiny::tags$p(figureText),
        shiny::downloadButton(ns("downloadCalibrationPlot"), "Save")
      ),
      shinycssloaders::withSpinner(reactable::reactableOutput(ns("nullDistribution")))
    )
  )
}

#' Calibration plot siny server module
#' @description
#' Display calibration plots for reward cohorts based on data available in system
#' Also shows Expected Absolute Systematic Error
#'
#' @param id                shiny namespace should be consistent with UI
#' @param model             RewardDataModel R6 class instance
#' @param selectedCohort    cohort object reactive - returned list must contain: cohortDefinitionId, isExposure, selectedOutcomeType (reactive)
#'
#' @export
calibrationPlotServer <- function(id, model, selectedCohort) {
  checkmate::assertR6(model, "RewardDataModel")
  checkmate::assert(shiny::is.reactive(selectedCohort))

  server <- shiny::moduleServer(id, function(input, output, session) {
    ParallelLogger::logInfo("Initialized calibration plot module")

    dataSources <- model$getDataSources()

    getNullDist <- shiny::reactive({
      cohort <- selectedCohort()
      nulls <- data.frame()
      negatives <- model$getNegativeControlSccResults(cohort$cohortDefinitionId,
                                                      cohort$isExposure,
                                                      outcomeType = cohort$selectedOutcomeType,
                                                      conceptSet = cohort$conceptSet)
      for (sourceId in unique(negatives$sourceId)) {
        subset <- negatives %>% dplyr::filter(.data$sourceId == sourceId,
                                              .data$analysisId == cohort$analysisId)
        null <- EmpiricalCalibration::fitNull(log(subset$rr), subset$seLogRr)
        systematicError <- EmpiricalCalibration::computeExpectedAbsoluteSystematicError(null)
        df <- data.frame(
          "sourceId" = sourceId,
          "n" = nrow(subset),
          "mean" = round(exp(null[["mean"]]), 3),
          "sd" = round(exp(null[["sd"]]), 3),
          "EASE" = round(systematicError, 3)
        )
        nulls <- rbind(nulls, df)
      }
      if (nrow(nulls))
        nulls <- dplyr::inner_join(dataSources, nulls, by = "sourceId")

      return(nulls)
    })

    getNullDistTable <- shiny::eventReactive(input$getNullDist, {
      nullDist <- getNullDist() %>%
        dplyr::select(.data$sourceName,
                      .data$sourceKey,
                      .data$n,
                      .data$mean,
                      .data$sd,
                      .data$EASE)
    })

    output$nullDistribution <- reactable::renderReactable({
      nullDist <- getNullDistTable()
      reactable::reactable(nullDist)
    })

    getCalibrationPlot <- shiny::reactive({
      cohort <- selectedCohort()

      plot <- ggplot2::ggplot()
      if (!is.null(cohort)) {
        null <- getNullDist()
        selectedRows <- input$nullDistribution_rows_selected
        validsourceIds <- null[selectedRows,]$sourceId

        negatives <- model$getNegativeControlSccResults(cohort$cohortDefinitionId,
                                                        cohort$isExposure,
                                                        outcomeType = cohort$selectedOutcomeType,
                                                        conceptSet = cohort$conceptSet)
        negatives <- negatives %>%
          dplyr::filter(.data$analysisId == cohort$analysisId)

        if (length(validsourceIds) == 0) {
          validsourceIds <- dataSources$sourceId[1]
        }
        negatives <- negatives[negatives$sourceId %in% validsourceIds,]

        if (nrow(negatives)) {
          plotNegatives <- negatives[negatives$rr > 0,]
          plot <- EmpiricalCalibration::plotCalibrationEffect(logRrNegatives = log(plotNegatives$rr),
                                                              seLogRrNegatives = plotNegatives$seLogRr)
        }
      }
      return(plot)
    })


    loadCalibrationPlot <- shiny::eventReactive(input$getNullDist, {
      getCalibrationPlot()
    })

    output$calibrationPlot <- plotly::renderPlotly({
      plot <- loadCalibrationPlot()
      return(plotly::ggplotly(plot))
    })

    output$downloadCalibrationPlot <- shiny::downloadHandler(
      filename = function() {
        "calibration-plot.png"
      },
      content = function(file) {
        ggplot2::ggsave(file, plot = getCalibrationPlot(), device = "png")
      }
    )
  })

  return(server)
}
