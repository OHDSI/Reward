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

strQueryWrap <- function(vec) {
  vec <- gsub("'", "''", vec)
  paste0("'", vec, "'", sep = "")
}

#' Wrapper around boxplot module
timeOnTreatmentServer <- function(id, model, selectedExposureOutcome) {
  caption <- "Table: Shows time on treatment for population od patients exposed to medication that experience the outcome of interest."
  server <- shiny::moduleServer(id, boxPlotModuleServer(model$getTimeOnTreatmentStats, caption, selectedExposureOutcome))
  return(server)
}

#' Wrapper around boxplot module
timeToOutcomeServer <- function(id, model, selectedExposureOutcome) {
  caption <- "Table: shows distribution of absolute difference of time between exposure and outcome for population of patients exposed to medication that expeirence the outcome."
  server <- shiny::moduleServer(id, boxPlotModuleServer(model$getTimeToOutcomeStats, caption, selectedExposureOutcome))
  return(server)
}

#' @title
#' Dashboard instance
#' @description
#' Requires a server appConfig instance to be loaded in environment see scoping of launchDashboard
#' This can be obtained with rewardb::loadappConfig(...)
#' @param input shiny input object
#' @param output shiny output object
#' @param session shiny session
#' @importFrom gt render_gt
#' @import shiny
rewardModule <- function(id = "Reward",
                         model) {
  appConfig <- model$config
  ns <- shiny::NS(id)
  shiny::onStop(function() { model$finalize() })

  shiny::moduleServer(id, function(input, output, session) {

    dataSourceInfo <- shiny::reactive({ model$getDataSourceInfo() })
    output$dataSourceTable <- gt::render_gt({
      tbl <- dataSourceInfo()
      colnames(tbl) <- SqlRender::camelCaseToTitleCase(colnames(tbl))
      tbl
    })

    output$requiredDataSources <- shiny::renderUI({
      shinyWidgets::pickerInput(ns("requiredDataSources"),
                                label = "Select required data sources for benefit:",
                                choices = dataSourceInfo()$sourceName,
                                options = shinyWidgets::pickerOptions(actionsBox = TRUE),
                                multiple = TRUE)
    })

    requiredBenefitSources <- shiny::reactive({
      dsi <- dataSourceInfo()
      dsi[dsi$sourceName %in% input$requiredDataSources,]$sourceId
    })

    getMainTableParams <- shiny::reactive({
      exposureClassNames <- if (!appConfig$exposureDashboard & length(input$exposureClass)) strQueryWrap(input$exposureClass) else NULL
      outcomeTypes <- input$outcomeCohortTypes

      params <- list(benefitThreshold = input$cutrange1,
                     riskThreshold = input$cutrange2,
                     pValueCut = input$pCut,
                     requiredBenefitSources = requiredBenefitSources(),
                     filterByMeta = input$filterThreshold == "Meta analysis",
                     outcomeCohortTypes = outcomeTypes,
                     calibrated = input$calibrated,
                     benefitCount = input$scBenefit,
                     riskCount = input$scRisk,
                     outcomeCohorts = input$outcomeCohorts,
                     targetCohorts = input$targetCohorts,
                     exposureClasses = exposureClassNames)

      return(params)
    })

    getMainTableCount <- shiny::reactive({
      params <- getMainTableParams()
      res <- do.call(model$getFilteredTableResultsCount, params)
      return(res)
    })

    mainTablePage <- shiny::reactiveVal(1)
    mainTableMaxPages <- shiny::reactive({
      recordCount <- getMainTableCount()
      ceiling(recordCount / as.integer(input$mainTablePageSize))
    })

    shiny::observeEvent(input$mainTableNext, {
      mainTablePage <- mainTablePage() + 1
      if (mainTablePage <= mainTableMaxPages()) {
        mainTablePage(mainTablePage)
      }

    })
    shiny::observeEvent(input$mainTablePrevious, {
      mainTablePage <- mainTablePage() - 1
      if (mainTablePage > 0) {
        mainTablePage(mainTablePage)
      }
    })

    getMainTablePage <- shiny::reactive({
      return(mainTablePage())
    })

    output$mainTablePage <- shiny::renderUI({

      numPages <- mainTableMaxPages()
      shiny::selectInput("mainTablePage", "Page", choices = 1:numPages, selected = mainTablePage())
    })

    shiny::observeEvent(input$mainTablePage, {
      mainTablePage(as.integer(input$mainTablePage))
    })

    output$mainTableNumPages <- shiny::renderText({
      recordCount <- getMainTableCount()
      numPages <- ceiling(recordCount / as.integer(input$mainTablePageSize))
      return(paste("Page", getMainTablePage(), "of", numPages))
    })

    output$mainTableCount <- shiny::renderText({
      res <- getMainTableCount()
      offset <- max(getMainTablePage() - 1, 0) * as.integer(input$mainTablePageSize) + 1
      endNum <- min(offset + as.integer(input$mainTablePageSize) - 1, res)
      str <- paste("Displaying", offset, "to", endNum, "of", res, "results")
      return(str)
    })

    exposureCohorts <- shiny::reactive({
      model$getExposureCohorts()
    }) %>% shiny::bindCache(paste0(appConfig$shortName, "-exposureCohorts"))

    outcomeCohorts <- shiny::reactive({
      model$getOutcomeCohorts()
    }) %>% shiny::bindCache(paste0(appConfig$shortName, "-outcomeCohorts"))

    shiny::observe({
      ocC <- outcomeCohorts()
      outcomeCohortChoices <- ocC$cohortDefinitioId
      names(outcomeCohortChoices) <- ocC$shortName

      ecC <- exposureCohorts()
      exposureCohortChoices <- ecC$cohortDefinitioId
      names(outcomeCohortChoices) <- ecC$shortName

      shiny::updateSelectizeInput(session, ns("outcomeCohorts"), choices = outcomeCohortChoices, server = TRUE)
      shiny::updateSelectizeInput(session, ns("targetCohorts"), choices = exposureCohortChoices, server = TRUE)

      if (!appConfig$exposureDashboard) {
        shiny::updateSelectizeInput(session, "exposureClass", choices = model$getExposureClassNames(), server = TRUE)
      }
    })

    # Subset of results for harm, risk and treatement categories
    # Logic: either select everything or select a user defined subset
    mainTableReac <- shiny::reactive({
      params <- getMainTableParams()
      params$limit <- input$mainTablePageSize
      params$offset <- max(getMainTablePage() - 1, 0) * as.integer(input$mainTablePageSize)
      params$orderByCol <- input$mainTableSortBy
      params$ascending <- input$mainTableOrderAscending
      do.call(model$getFilteredTableResults, params)
    })

    output$mainTable <- DT::renderDataTable({
      df <- mainTableReac()
      if (length(df$I2)) {
        df$I2 <- formatC(df$I2, digits = 2, format = "f")
      }

      colnames(df) <- SqlRender::camelCaseToTitleCase(colnames(df))

      table <- DT::datatable(
        df, selection = "single", options = list(dom = 't', pageLength = input$mainTablePageSize, ordering = F),
        rownames = FALSE
      )
      return(table)
    })

    # This links the app components together
    selectedExposureOutcome <- shiny::reactive({
      ids <- input$mainTable_rows_selected
      filtered1 <- mainTableReac()

      if (!length(ids)) {
        return(NULL)
      }

      filtered2 <- filtered1[ids,]
      filtered2$calibrationType <- "none"
      return(filtered2)
    })

    metaAnalysisTableServer("metaTable", model, selectedExposureOutcome)
    forestPlotServer("forestPlot", model, selectedExposureOutcome)
    calibrationPlotServer("calibrationPlot", model, selectedExposureOutcome)

    timeOnTreatmentServer("timeOnTreatment", model, selectedExposureOutcome)
    tabPanelTimeOnTreatment <- tabPanel("Time on treatment", boxPlotModuleUi("timeOnTreatment"))
    shiny::appendTab(inputId = "outcomeResultsTabs", tabPanelTimeOnTreatment)

    timeToOutcomeServer("timeToOutcome", model, selectedExposureOutcome)
    tabPanelTimeToOutcome <- tabPanel("Time to outcome", boxPlotModuleUi("timeToOutcome"))
    shiny::appendTab(inputId = "outcomeResultsTabs", tabPanelTimeToOutcome)

    fullDataDownload <- shiny::reactive({
      model$getFilteredTableResults(benefitThreshold = input$cutrange1,
                                    riskThreshold = input$cutrange2,
                                    pValueCut = input$pCut,
                                    filterByMeta = input$filterThreshold == "Meta analysis",
                                    calibrated = input$calibrated,
                                    benefitCount = input$scBenefit,
                                    riskCount = input$scRisk)
    })

    output$treatmentOutcomeStr <- shiny::renderText({
      s <- selectedExposureOutcome()
      return(paste(s$TARGET_COHORT_NAME, "for", s$OUTCOME_COHORT_NAME))
    })

    output$downloadData <- shiny::downloadHandler(
      filename = function() {
        paste0(appConfig$short_name, '-full_results', input$cutrange1, '-', input$cutrange2, '.csv')
      },
      content = function(file) {
        write.csv(fullDataDownload(), file, row.names = FALSE)
      }
    )

    output$downloadFullData <- shiny::downloadHandler(
      filename = function() {
        paste0(appConfig$short_name, '-export.csv')
      },
      content = function(file) {
        data <- model$getFullDataSet()
        write.csv(data, file, row.names = FALSE)
      })

    getNegativeControls <- shiny::reactive({
      model$getNegativeControls()
    })

    output$downloadControls <- downloadHandler(
      filename = function() {
        paste0(appConfig$short_name, '-negative-controls.csv')
      },
      content = function(file) {
        write.csv(getNegativeControls(), file, row.names = FALSE)
      })

    getIndications <- shiny::reactive({
      model$getMappedAssociations()
    })

    output$downloadIndications <- shiny::downloadHandler(
      filename = function() {
        paste0(appConfig$short_name, '-indications.csv')
      },
      content = function(file) {
        write.csv(getIndications(), file, row.names = FALSE)
      }
    )

    # Subset without limit
    mainTableDownload <- shiny::reactive({
      params <- getMainTableParams()
      do.call(model$getFilteredTableResults, params)
    })

    output$downloadFullTable <- shiny::downloadHandler(
      filename = function() {
        paste0(appConfig$short_name, '-filtered-', input$cutrange1, '-', input$cutrange2, '.csv')
      },
      content = function(file) {
        write.csv(mainTableDownload(), file, row.names = FALSE)
      }
    )

    ingredientConetpInput <- shiny::reactive({
      selected <- selectedExposureOutcome()
      if (is.null(selected))
        return(data.frame())
      model$getExposureConceptSet(selected$TARGET_COHORT_ID)
    })

    conditionConceptInput <- shiny::reactive({
      selected <- selectedExposureOutcome()
      if (is.null(selected))
        return(data.frame())
      model$getOutcomeConceptSet(selected$OUTCOME_COHORT_ID)
    })


    output$selectedOutcomeConceptSet <- DT::renderDataTable({ conditionConceptInput() })
    output$selectedExposureConceptSet <- DT::renderDataTable({ ingredientConetpInput() })

    # Add cem panel if option is present
    if (!is.null(appConfig$cemConnectionDetails)) {
      message("loading cem api")
      cemBackend <- do.call(CemConnector::createCemConnection, appConfig$cemConnectionDetails)
      ceModuleServer <- CemConnector::ceExplorerModule("cemExplorer",
                                                       cemBackend,
                                                       ingredientConceptInput = ingredientConetpInput,
                                                       conditionConceptInput = conditionConceptInput,
                                                       siblingLookupLevelsInput = shiny::reactive({ 0 }))
      cemPanel <- shiny::tabPanel("Evidence", CemConnector::ceExplorerModuleUi("cemExplorer"))
      shiny::appendTab(inputId = "outcomeResultsTabs", cemPanel)
    }
  })
}

dashboardInstance <- function(input,
                              output,
                              session,
                              dashboardConfigPath = .GlobalEnv$dashboardConfigPath,
                              connectionDetails = .GlobalEnv$connectionDetails,
                              resultDatabaseSchema = .GlobalEnv$resultDatabaseSchema,
                              globalConfigPath = .GlobalEnv$configPath) {

  if (is.null(connectionDetails)) {
    config <- loadGlobalConfiguration(globalConfigPath)
    connectionDetails <- config$connectionDetails
  }

  model <- DashboardDataModel$new(dashboardConfigPath = dashboardConfigPath,
                                  connectionDetails = connectionDetails,
                                  resultDatabaseSchema = resultDatabaseSchema)

  rewardModule(model = model)
}

dashboardUi <- function(...) {
  rewardUi(appConfig = loadDashboardConfiguration(.GlobalEnv$dashboardConfigPath))
}

#' @title
#' Launch the REWARD Shiny app dashboard
#' @description
#' Launches a Shiny app for a given configuration file
#' @param appConfigPath path to configuration file. This is loaded in to the local environment with the appConfig variable
#'
#' @import shiny
#' @export
launchDashboard <- function(dashboardConfigPath,
                            configPath = NULL,
                            connectionDetails = NULL,
                            resultDatabaseSchema = NULL) {

  if (all(is.null(c(configPath, connectionDetails)))) {
    stop("Must specify config path or connectionDetails")
  }

  # These are probably null but shiny launch is not easy to modify
  # .configPath <- .GlobalEnv$configPath
  # .dashboardConfigPath <- .GlobalEnv$dashboardConfigPath
  # .connectionDetails <- .GlobalEnv$connectionDetails
  # .resultDatabaseSchema <- .GlobalEnv$resultDatabaseSchema
  # # Storing masked variables in a temporary location
  # on.exit({
  #   .GlobalEnv$configPath <- .configPath
  #   .GlobalEnv$dashboardConfigPath <- .dashboardConfigPath
  #   .GlobalEnv$connectionDetails <- .connectionDetails
  #   .GlobalEnv$resultDatabaseSchema <- .resultDatabaseSchema
  # }, add = TRUE)

  .GlobalEnv$configPath <- configPath
  .GlobalEnv$dashboardConfigPath <- normalizePath(dashboardConfigPath)
  .GlobalEnv$connectionDetails <- connectionDetails

  if (!is.null(connectionDetails) && connectionDetails$dbms == "sqlite") {
    resultDatabaseSchema <- "main"
  } else if (is.null(resultDatabaseSchema)) {
    stop("must specify result schema")
  }

  .GlobalEnv$resultDatabaseSchema <- resultDatabaseSchema
  shiny::shinyApp(server = dashboardInstance, dashboardUi, enableBookmarking = "url")
}