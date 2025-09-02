getResultsConnectionDetails <- function(config = config::get()) {
  do.call(DatabaseConnector::createConnectionDetails, config$resultsConnectionDetails)
}

createDashboardResults <- function(config = config::get(), dashboard) {
  # Get negative control pairs:
  negatives <- getNegativeControlPairs(config, dashboard)

  connectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = config::get("dbms"),
    user = config::get("user"),
    password = config::get("password"),
    connectionString = config::get("connectionString")
  )
  # Run SCC for dashboard
  targetCohortIds <- purrr::map(dashboard$config$cohortConceptIds, function(cohort) {
    cohort$cohortId
  }) |> as.numeric()

  targetCohortIds <- c(targetCohortIds, dashboard$config$targetConceptIds * 1000)

  if (dashboard$config$dashboardType == "exposure") {
    controlType <- "outcome"
    outcomeCohortIds <- getOutcomeCohortIds()
    exposureCohortIds <- targetCohortIds
  } else {
    controlType <- "exposure"
    exposureCohortIds <- getExposureCohortIds()
    outcomeCohortIds <- targetCohortIds
  }
  datasources <- config$datasources
  purrr::walk(datasources, function(datasource) {
    executionSettings <- config::get(config = datasource) |>
      purrr::discard_at(c("dbms", "user", "password", "connectionString"))

    execSccAnalyses(connectionDetails,
                    executionSettings,
                    dashboard = dashboard,
                    config = config,
                    exposureCohortIds = exposureCohortIds,
                    outcomeCohortIds = outcomeCohortIds,
                    negativeControls = negatives,
                    controlType = controlType)
  })
}


uploadDashboardData <- function(config = config::get(), dashboard) {
  # Cohort Generator results files for each db
  connectionDetails <- getResultsConnectionDetails(config)
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  res <- askYesNo(glue::glue("This will delete and recreate the schema {dashboard$config$databaseSchema}, continue?"))
  if (!isTRUE(res))
    return(invisible())

  DatabaseConnector::renderTranslateExecuteSql(connection,
                                               "DROP SCHEMA IF EXISTS @dash_schema CASCADE;
                                               CREATE SCHEMA @dash_schema;",
                                               dash_schema = dashboard$config$databaseSchema)

  SelfControlledCohort::createResultsDataModel(connectionDetails, dashboard$config$databaseSchema)
  CohortGenerator::createResultsDataModel(connectionDetails, dashboard$config$databaseSchema)

  # SCC results
  datasources <- config$datasources
  for (datasource in datasources) {
    executionSettings <- config::get(config = datasource) |>
      purrr::discard_at(c("dbms", "user", "password", "connectionString"))

    resultsPath <- file.path("exec", "results", executionSettings$databaseId, dashboard$config$databaseSchema, "scc_result")
    for (asetting in getSccAnalysisList()) {

      sccResultsPath <- file.path(resultsPath, paste0("A_", asetting$analysisId))
      cli::cli_alert_info("Uploading from {sccResultsPath}")
      SelfControlledCohort::uploadResults(connectionDetails = connectionDetails,
                                          schema = dashboard$config$databaseSchema,
                                          resultsFolder = sccResultsPath,
                                          forceOverWriteOfSpecifications = TRUE,
                                          purgeSiteDataBeforeUploading = FALSE)
    }
    cli::cli_alert_success("Upload for {executionSettings$databaseId} complete")
  }

  cli::cli_alert_success("Upload for scc data complete")
}


copyCgTables <- function(config = config::get(), dashboard) {
  connectionDetails <- getResultsConnectionDetails(config)
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  tmpTableSql <- "
  DROP TABLE IF EXISTS #copy_table;
  CREATE TABLE #copy_table AS
  SELECT DISTINCT outcome_cohort_id as cohort_definition_id FROM @schema.scc_outcome_exposure
  UNION
  SELECT DISTINCT target_cohort_id as cohort_definition_id FROM @schema.scc_outcome_exposure;

  -- TODO: remove these when tables are added to cohort generator
  DROP TABLE IF EXISTS @schema.cg_concept_set_name;
  CREATE TABLE @schema.cg_concept_set_name(
    concept_set_name varchar,
    concept_set_id varchar(256)
    --PRIMARY KEY (concept_set_id, concept_set_name)
  );

  DROP TABLE IF EXISTS @schema.cg_concept_set;
  CREATE TABLE @schema.cg_concept_set(
      concept_id bigint,
      include_mapped int,
      include_descendants int,
      is_excluded int,
      concept_set_id varchar(256)
  );

  DROP TABLE IF EXISTS @schema.cg_cohort_concept_set;
  CREATE TABLE @schema.cg_cohort_concept_set(
      concept_set_id varchar (256),
      cohort_definition_id bigint
      --PRIMARY KEY (concept_set_id, cohort_definition_id)
  );

  "
  DatabaseConnector::renderTranslateExecuteSql(connection,
                                               tmpTableSql,
                                               schema = dashboard$config$databaseSchema)

  # Get CG results for just the cohorts in the dashboard project
  cgSpecs <- CohortGenerator::getResultsDataModelSpecifications()
  copyTables <- c(cgSpecs$tableName, "cg_concept_set_name", "cg_concept_set", "cg_cohort_concept_set") |> unique()
  purrr::walk(copyTables, function(tableName) {
    specRows <- cgSpecs |> dplyr::filter(.data$tableName == !!tableName)

    sql <- "
    TRUNCATE TABLE @dashboard_schema.@table_name;
    INSERT INTO @dashboard_schema.@table_name
    SELECT * FROM @main_schema.@table_name t
    {@pull_cohorts} ? {INNER JOIN #copy_table ct ON t.cohort_definition_id = ct.cohort_definition_id}
    "
    DatabaseConnector::renderTranslateExecuteSql(connection,
                                                 sql,
                                                 table_name = tableName,
                                                 main_schema = config$resultsDatabaseSchema,
                                                 pull_cohorts = "cohort_definition_id" %in% specRows,
                                                 progressBar = FALSE,
                                                 dashboard_schema = dashboard$config$databaseSchema)
  })
  cli::cli_alert_success("copied cohort generator tables")
  invisible()
}

createDatabaseMetaInfo <- function(config = config::get(), dashboard) {

  datasources <- config$datasources

  connectionDetails <- getResultsConnectionDetails(config)
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)

  connectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = config::get("dbms"),
    user = config::get("user"),
    password = config::get("password"),
    connectionString = config::get("connectionString")
  )

  cdmConnection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(cdmConnection), add = TRUE)

  cdmSources <- data.frame()
  purrr::walk(datasources, function(datasource) {
    executionSettings <- config::get(config = datasource) |>
      purrr::discard_at(c("dbms", "user", "password", "connectionString"))

    sql <- "SELECT TOP 1 * FROM @cdm_database_schema.cdm_source;"
    cdmSource <- DatabaseConnector::renderTranslateQuerySql(
      connection = cdmConnection,
      sql = sql,
      snakeCaseToCamelCase = TRUE,
      cdm_database_schema = executionSettings$cdmDatabaseSchema
    )

    cdmSource$databaseId <- executionSettings$databaseId
    cdmSources <<- rbind(cdmSources, cdmSource)
  })

  DatabaseConnector::insertTable(connection,
                                 databaseSchema = dashboard$config$databaseSchema,
                                 tableName = "database_meta_data",
                                 data = cdmSources,
                                 dropTableIfExists = TRUE,
                                 createTable = TRUE,
                                 tempTable = FALSE,
                                 tempEmulationSchema = NULL,
                                 bulkLoad = FALSE,
                                 progressBar = FALSE,
                                 camelCaseToSnakeCase = TRUE)
}


createDashboard <- function(dashboardName, config = config::get()) {
  dashboard <- getDashboardConfig(dashboardName)

  cli::cli_h1("creating dashboard {dashboardName}")
  createDashboardResults(config, dashboard)
  uploadDashboardData(config, dashboard)
  copyCgTables(config, dashboard)
  createDatabaseMetaInfo(config, dashboard)
  calibratedMetaAnalysisResults(config, dashboard)

  cli::cli_alert_success("dashboard dataset sucessfully created at {dashboard$config$databaseSchema}")
}

launchShiny <- function(dashboardName, config = config::get()) {
  devtools::load_all("../SelfControlledCohort")
  dashboard <- getDashboardConfig(dashboardName)
  SelfControlledCohort::launchDashboard(getResultsConnectionDetails(config = config),
                                        dashboardConfig = dashboard$getShinyConfig())
}

getDashboardDataModel <- function(dashboardName, config = config::get()) {
  dashboard <- getDashboardConfig(dashboardName)
  connectionHandler <- ResultModelManager::ConnectionHandler$new(getResultsConnectionDetails(config = config))
  model <- SelfControlledCohort:::SccDataModel$new(connectionHandler, dashboard$getShinyConfig())
  return(model)
}


deployDashboard <- function(dashboardName, config = config::get()) {
  dashboard <- getDashboardConfig(dashboardName)
  dpath <- file.path("dash_deploy", dashboard$config$databaseSchema)
  dir.create(dpath, showWarnings = FALSE, recursive = TRUE)
  file.copy(file.path("analysis", "shiny", "app.R"), file.path("dash_deploy", dashboard$config$databaseSchema), overwrite = TRUE)
  file.copy(dashboard$filePath, file.path("dash_deploy", dashboard$config$databaseSchema, "config.yml"), overwrite = TRUE)
  DatabaseConnector::downloadJdbcDrivers("postgresql", dpath)
  envvars <- glue::glue("
REWARD_DB_SERVER = {Sys.getenv('REWARD_DB_SERVER')}
REWARD_DB_USER = {Sys.getenv('REWARD_DB_USER')}
REWARD_DB_PASSWORD = {Sys.getenv('REWARD_DB_PASSWORD')}
  ")
  writeLines(envvars, file.path(dpath, ".Renviron"))
  on.exit(unlink(file.path(dpath, ".Renviron")))

  rsconnect::deployApp(appDir = dpath,
                       appTitle = dashboard$config$name)
}
