cmStudyUi <- function(id = "cmStudyDesigner") {
  ns <- shiny::NS(id)
  shiny::tagList(
    shinydashboard::box(
      width = NULL,
      shiny::titlePanel("Cohort Method Study design"),
      shiny::p("Design a Cohort Method comparative cohort study design for use in reward"),
      shinyWidgets::virtualSelectInput(inputId = ns("target"), label = "Target", multiple = FALSE, choices = c(), selected = NULL, search = TRUE, disabled = TRUE),
      shinyWidgets::virtualSelectInput(inputId = ns("comparator"), label = "Comparator", multiple = FALSE, choices = c(), selected = NULL, search = TRUE, disabled = TRUE),
      shinyWidgets::virtualSelectInput(inputId = ns("outcomes"), label = "Outcomes", multiple = TRUE, choices = c(), selected = NULL, search = TRUE, disabled = TRUE),
      shinyWidgets::virtualSelectInput(inputId = ns("dataSources"), label = "Data Sources", multiple = TRUE, choices = c(), selected = NULL, search = TRUE, disabled = TRUE),
      # input exclusion covariates text area
      shiny::textAreaInput(inputId = ns("exclusionCovariates"), label = "Exclusion covariate", placeholder = "Covariate ids to exclude. Separate with comma"),
      # Generate json button

      shiny::actionButton(inputId = ns("showJsonConfig"), "Create JSON config")
    ),
    shinydashboard::box(
      width = NULL,
      # shiny::conditionalPanel(
      #   conditon = "input.showJsonConfig > 1",
      #   ns = ns,
      shiny::p("Add the following settings object to a strategus study"),
      shiny::div(
        shiny::verbatimTextOutput(outputId = ns("studyJson")),
        style = "height:500px; overflow:scroll;"
      ),
      shiny::downloadButton(outputId = ns("downloadStudyJson"), label = "Download JSON")
      # )
    )
  )
}


cmStudyModule <- function(id = "cmStudyDesigner", model) {
  shiny::moduleServer(id, function(input, output, session) {
    shiny::withProgress(
    {
      dataSourcesTbl <- model$getDataSources()
      shiny::setProgress(session = session, value = 10)
      targetsTbl <- model$getExposureCohortDefinitionSet()
      shiny::setProgress(session = session, value = 20)
      outcomesTbl <- model$getOutcomeCohortDefinitionSet()
      shiny::setProgress(session = session, value = 30)

      targets <- shinyWidgets::prepare_choices(targetsTbl, label = .data$shortName, value = .data$cohortDefinitionId)
      shiny::setProgress(session = session, value = 50)
      outcomes <- shinyWidgets::prepare_choices(outcomesTbl, label = .data$shortName, value = .data$cohortDefinitionId)
      shiny::setProgress(session = session, value = 80)
      dataSources <- shinyWidgets::prepare_choices(dataSourcesTbl, label = .data$sourceName, value = .data$sourceId)
      shiny::setProgress(session = session, value = 100)
    }, message = "Loading cohorts...",
      session = session)


    shiny::observe({
      shiny::withProgress(
      {
        shinyWidgets::updateVirtualSelect(inputId = "target", choices = targets, disable = FALSE)
        shiny::setProgress(session = session, value = 33)
        shinyWidgets::updateVirtualSelect(inputId = "comparator", choices = targets, disable = FALSE)
        shiny::setProgress(session = session, value = 66)
        shinyWidgets::updateVirtualSelect(inputId = "outcomes", choices = outcomes, disable = FALSE)
        shiny::setProgress(session = session, value = 90)
        shinyWidgets::updateVirtualSelect(inputId = "dataSources", choices = dataSources, selected = dataSources, disable = FALSE)
        shiny::setProgress(session = session, value = 100)
      }, message = "Setting inputs...",
        session = session)
    })

    getSpec <- shiny::eventReactive(input$showJsonConfig, {
      targetId <- as.numeric(input$target)
      compartatorId <- as.numeric(input$comparator)
      dataSources <- input$dataSources
      outcomes <- as.numeric(input$outcomes)
      excludedCovariateConceptIds <- NULL
      if (!is.null(input$exclusionCovariates)) {
        exclusionCovariates <- strsplit(input$exclusionCovariates, ",")[[1]]
        excludedCovariateConceptIds <- as.numeric(exclusionCovariates)
      }
      # TODO: validate inputs and display error message (or disable load button)

      spec <- createCmDesign(targetId = targetId,
                             comparatorId = compartatorId,
                             indicationId = NULL,
                             outcomeCohortIds = outcomes,
                             dataSources = dataSources,
                             excludedCovariateConceptIds = excludedCovariateConceptIds)

      return(spec)
    })


    output$studyJson <- shiny::renderText({
      getSpec() %>% ParallelLogger::convertSettingsToJson()
    })

  })
}