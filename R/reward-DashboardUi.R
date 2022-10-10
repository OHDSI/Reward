#' Ui for rewardb dashboard
#' @param request shiny request object
#' @importFrom gt gt_output
#' @import shiny
#' @import shinyWidgets
#' @importFrom shinycssloaders withSpinner
#' @import shinydashboard
#' @export
rewardUi <- function(id = "Reward",
                     appConfig) {
  ns <- shiny::NS(id)
  # This hides the outcome exporues/result pairing
  metaDisplayCondtion <- "typeof input.mainTable_rows_selected  !== 'undefined' && input.mainTable_rows_selected.length > 0"

  filterBox <- shinydashboard::box(
    shinydashboard::box(
      shiny::selectizeInput(
        inputId = ns("targetCohorts"),
        label = "Drug exposures:",
        choices = NULL,
        multiple = TRUE),
      shiny::selectizeInput(
        inputId = ns("outcomeCohorts"),
        label = "Disease outcomes:",
        choices = NULL,
        multiple = TRUE)),
    shinydashboard::box(
      shiny::selectizeInput(
        inputId = ns("exposureClass"),
        label = "Drug exposure classes:",
        choices = NULL,
        multiple = TRUE),
      shinyWidgets::pickerInput(
        inputId = ns("outcomeCohortTypes"),
        "Outcome Cohort Types:",
        choices = c("ATLAS defined" = 3, "Inpatient" = 1, "Two diagnosis codes" = 0, "One diagnosis code" = 2),
        selected = c(),
        options = shinyWidgets::pickerOptions(
          actionsBox = TRUE,
          noneSelectedText = "Filter by subset"
        ),
        multiple = TRUE),
      width = 6
    ),
    width = 12,
    title = "Filter Cohorts",
    collapsible = TRUE
  )

  mainResults <- shinydashboard::box(
    shiny::fluidRow(
      shiny::column(
        width = 2,
        shiny::uiOutput(ns("mainTablePage"))
      ),
      shiny::column(
        width = 6,
        shiny::selectInput(
          inputId = ns("mainTableSortBy"),
          "Sort by column",
          choices = list(
            "Outcome id" = "OUTCOME_COHORT_ID",
            "Exposure id" = "TARGET_COHORT_ID",
            "Exposure name" = "TARGET_COHORT_NAME",
            "Outcome name" = "OUTCOME_COHORT_NAME",
            "I-squared" = "I2",
            "IRR" = "META_RR",
            "Sources with scc risk" = "RISK_COUNT",
            "Sources with scc benefit" = "BENEFIT_COUNT"),
          selected = "META_RR"
        )
      ),
      shiny::column(
        width = 2,
        shiny::radioButtons(ns("mainTableOrderAscending"), "", c("Ascending" = "ASC", "Descending" = "DESC"))),
      shiny::column(
        width = 2,
        shiny::selectInput(
          ns("mainTablePageSize"),
          "Show per page",
          choices = c(5, 10, 15, 20, 25, 50, 100), selected = 10
        )
      )
    ),
    shinycssloaders::withSpinner(DT::dataTableOutput(ns("mainTable"))),
    shiny::hr(),
    shiny::fluidRow(
      shiny::column(
        width = 4,
        shiny::textOutput(ns("mainTableCount")),
        shiny::actionButton(ns("mainTablePrevious"), "Previous Page")
      ),
      shiny::column(width = 6),
      shiny::column(
        width = 2,
        shiny::textOutput(ns("mainTableNumPages")),
        shiny::actionButton(ns("mainTableNext"), "Next Page"))),
    shiny::hr(),
    shiny::downloadButton(ns("downloadFullTable"), "Download"),
    width = 12
  )

  rPanel <- shiny::conditionalPanel(
    condition = metaDisplayCondtion,
    ns = ns,
    shinydashboard::box(
      shiny::HTML(paste("<h4 id='mainR'>", textOutput("treatmentOutcomeStr"), "</h4>")),
      shiny::tabsetPanel(
        id = "outcomeResultsTabs",
        shiny::tabPanel("Detailed results", metaAnalysisTableUi("metaTable")),
        shiny::tabPanel("Forest plot", forestPlotUi("forestPlot")),
        shiny::tabPanel("Calibration plot",
                        calibrationPlotUi("calibrationPlot",
                                          figureTitle = "Figure 2.")),
        shiny::tabPanel(
          "Exposure concepts",
          shiny::h4("Exposure Concepts"),
          shinycssloaders::withSpinner(DT::dataTableOutput("selectedExposureConceptSet"))),
        shiny::tabPanel(
          "Outcome concepts",
          h4("Outcome Concepts"),
          shinycssloaders::withSpinner(DT::dataTableOutput("selectedOutcomeConceptSet")))),
      width = 12)
  )

  aboutTab <- shiny::tagList(
    shinydashboard::box(
      shiny::includeHTML(system.file("html", "about.html", package = "Reward")),
      width = 12,
      title = paste("Real World Assessment and Research of Drug performance (REWARD)")),
    shinydashboard::box(
      width = 6,
      title = "Data sources",
      shinycssloaders::withSpinner(gt::gt_output(outputId = ns("dataSourceTable")))),
    shinydashboard::box(
      shiny::p(appConfig$description),
      shiny::p("Click the dashboard option to see the results. The sidebar options allow filtering of results based on risk and benift IRR thresholds"),
      shiny::downloadButton(
        ns("downloadData"),
        "Download filtered results as a csv"),
      shiny::downloadButton(
        ns("downloadFullData"),
        "Download full results"),
      width = 6,
      title = paste("About this dashboard -", appConfig$dashboardName
      )
    )
  )

  body <- shinydashboard::dashboardBody(
    shinydashboard::tabItems(
      shinydashboard::tabItem(tabName = "about", aboutTab),
      shinydashboard::tabItem(tabName = "results", shiny::fluidRow(filterBox, mainResults, rPanel))
    )
  )

  sidebar <- shinydashboard::dashboardSidebar(
    shinydashboard::sidebarMenu(
      shinydashboard::menuItem("About", tabName = "about", icon = icon("rectangle-list")),
      shinydashboard::menuItem("Results", tabName = "results", icon = icon("table")),
      shiny::sliderInput(ns("cutrange1"), "Benefit Threshold:", min = 0.1, max = 0.9, step = 0.1, value = 0.5),
      shiny::sliderInput(ns("cutrange2"), "Risk Threshold:", min = 1.1, max = 2.5, step = 0.1, value = 2),
      shiny::sliderInput(ns("pCut"), "P-value cut off:", min = 0.0, max = 1.0, step = 0.01, value = 0.05),
      shiny::checkboxInput(ns("calibrated"), "Threshold with empirically calibrated IRR", TRUE),
      shiny::radioButtons(ns("filterThreshold"), "Threshold benefit by:", c("Data sources", "Meta analysis")),
      shiny::sliderInput(
        ns("scBenefit"),
        "Minimum sources with self control benefit:",
        min = 0,
        max = 5,
        step = 1,
        value = 1),
      shiny::sliderInput(
        ns("scRisk"),
        "Maximum sources with self control risk:",
        min = 0,
        max = 5,
        step = 1,
        value = 0
      ),
      shinycssloaders::withSpinner(shiny::uiOutput(ns("requiredDataSources"))),
      shiny::bookmarkButton()))

  appTitle <- paste(appConfig$dashboardName)
  # Put them together into a dashboardPage
  ui <- shinydashboard::dashboardPage(shinydashboard::dashboardHeader(title = appTitle),
                                      sidebar,
                                      body)
  return(ui)
}