metaAnalysisTableUi <- function(id) {
  shiny::tagList(
    shinycssloaders::withSpinner(DT::dataTableOutput(NS(id, "fullResultsTable"))),
    shiny::hr(),
    shiny::downloadButton(NS(id, "downloadSubTable"), "Download table")
  )
}

metaAnalysisTableServer <- function(id, model, selectedExposureOutcome) {
  library(shiny)
  niceColumnName <- list(
    SOURCE_NAME = "Database",
    CALIBRATED_RR = "Relative Risk *calibrated",
    CALIBRATED_CI_95 = "CI 95*",
    CALIBRATED_P_VALUE = "P*",
    RR = "Relative Risk",
    CI_95 = "CI 95",
    P_VALUE = "P",
    T_AT_RISK = "N Exp",
    T_PT = "Exposed time (years)",
    C_CASES = "Unexposed cases",
    T_CASES = "Exposed cases"
  )

  niceColumnNameInv <- list()

  for (n in names(niceColumnName)) {
    niceColumnNameInv[niceColumnName[[n]]] <- n
  }

  server <- moduleServer(id, function(input, output, session) {

    metaAnalysisTbl <- reactive({
      s <- selectedExposureOutcome()
      exposureId <- s$TARGET_COHORT_ID
      outcomeId <- s$OUTCOME_COHORT_ID
      calibrationType <- s$calibrationType
      if (length(outcomeId) & length(exposureId)) {
        return(model$getMetaAnalysisTable(exposureId, outcomeId, calibrationType = calibrationType, sourceIds = s$usedDataSources))
      }
      return(data.frame())
    })

    fullResultsTable <- reactive({
      table3 <- metaAnalysisTbl()

      if (length(table3) == 0) {
        return(data.frame())
      }

      if (nrow(table3) >= 1) {
        table3$RR[table3$RR > 100] <- NA
        table3$C_PT <- formatC(table3$C_PT, digits = 0, format = "f")
        table3$T_PT <- formatC(table3$T_PT, digits = 0, format = "f")
        table3$RR <- formatC(table3$RR, digits = 2, format = "f")
        table3$LB_95 <- formatC(table3$LB_95, digits = 2, format = "f")
        table3$UB_95 <- formatC(table3$UB_95, digits = 2, format = "f")
        table3$P_VALUE <- formatC(table3$P_VALUE, digits = 2, format = "f")

        table3$CALIBRATED_RR <- formatC(table3$CALIBRATED_RR, digits = 2, format = "f")
        table3$CALIBRATED_LB_95 <- formatC(table3$CALIBRATED_LB_95, digits = 2, format = "f")
        table3$CALIBRATED_UB_95 <- formatC(table3$CALIBRATED_UB_95, digits = 2, format = "f")
        table3$CALIBRATED_P_VALUE <- formatC(table3$CALIBRATED_P_VALUE, digits = 2, format = "f")

        for (n in names(niceColumnName)) {
          colnames(table3)[colnames(table3) == n] <- niceColumnName[n]
        }

        headers <- names(niceColumnNameInv)
        table4 <- DT::datatable(
          table3[, headers], rownames = FALSE, escape = FALSE, options = list(dom = 't'),
          caption = "* Indicates values after empirical calibration"
        )
        return(table4)
      }
    })

    output$fullResultsTable <- DT::renderDataTable({
      tryCatch({
        return(fullResultsTable())
      },
      error = function(e) {
        ParallelLogger::logError(e)
        return(data.frame())
      })
    })

    output$downloadSubTable <- downloadHandler(
      filename = function() {
        s <- selectedExposureOutcome()
        treatment <- s$TARGET_COHORT_ID
        outcome <- s$OUTCOME_COHORT_ID
        paste0(model$schemaName, '-results-', treatment, "-", outcome, '.csv')
      },
      content = function(file) {
        write.csv(metaAnalysisTbl(), file, row.names = FALSE)
      }
    )
  })
}