# NOTE the functions in this file will be included in the main SCC package as soon as a strategus module is developed
metaAnalysis <- function(table, ...) {
  meta::settings.meta('meta4')
  table$i2 <- NA

  results <- meta::metainc(data = table,
                           event.e = abs(numOutcomesExposed),
                           time.e = timeAtRiskExposed,
                           event.c = abs(numOutcomesUnexposed),
                           time.c = timeAtRiskUnexposed,
                           sm = "IRR",
                           model.glmm = "UM.RS")

  # Return a single row with computed results
  row <- data.frame(databaseId = "meta-analysis",
                    targetCohortId = table$targetCohortId[1],
                    outcomeCohortId = table$outcomeCohortId[1],
                    analysisId = table$analysisId[1],
                    numPersons = sum(table$numPersons),
                    timeAtRiskExposed = sum(table$timeAtRiskExposed),
                    numExposures = sum(table$numExposures),
                    numOutcomesExposed = sum(table$numOutcomesExposed),
                    timeAtRiskUnexposed = sum(table$timeAtRiskUnexposed),
                    numOutcomesUnexposed = sum(table$numOutcomesUnexposed),
                    rr = exp(results$TE.random),
                    logRr = results$TE.random,
                    seLogRr = results$seTE.random,
                    lb95 = exp(results$lower.random),
                    ub95 = exp(results$upper.random),
                    pValue = results$pval.random,
                    i2 = ifelse(is.na(results$I2), 0.0, results$I2))

  return(row)
}


calibratedMetaAnalysisResults <- function(config = config::get(), dashboard) {
  # get positive/negative rows from results db
  future::plan(config$futurePlanningComittee, workers = config$computeThreads)
  resultsPath <- file.path("exec", "results", "meta_analysis", dashboard$config$databaseSchema, "scc_result")
  resultExportManager <- SelfControlledCohort::getDefaultExportManager(resultsPath, databaseId = "meta-analysis")

  connectionDetails <- getResultsConnectionDetails(config)
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  cli::cli_alert_info("Starting meta-analysis computation")
  # Run meta-analysis, write intermediate file
  metaFile <- file.path(resultsPath, "meta_scc_step.csv")

  if (!file.exists(metaFile)) {
    allResults <- DatabaseConnector::renderTranslateQuerySql(connection,
                                                             sql = "SELECT sr.* FROM @database_schema.scc_result sr",
                                                             snakeCaseToCamelCase = TRUE,
                                                             database_schema = dashboard$config$databaseSchema)

    metaResults <- allResults |>
      dplyr::group_by(.data$targetCohortId,
                      .data$outcomeCohortId,
                      .data$analysisId) |>
      dplyr::group_split() |>
      furrr::future_map_dfr(~metaAnalysis(.x))

    metaResults |>
      readr::write_csv(metaFile)
  } else {
    metaResults <- readr::read_csv(metaFile, col_types = readr::cols())
  }

  negativeControls <- DatabaseConnector::renderTranslateQuerySql(connection,
                                                                 sql = "SELECT eo.* FROM @database_schema.scc_outcome_exposure eo WHERE true_effect_size = 1",
                                                                 snakeCaseToCamelCase = TRUE,
                                                                 database_schema = dashboard$config$databaseSchema)

  negatives <- purrr::map(unique(metaResults$analysisId), function(a) {
    negativeControls$analysisId <- a
    negativeControls
  }) |> dplyr::bind_rows()

  cli::cli_alert_info("Computing calibrated meta-analysis results")

  if (dashboard$config$dashboardType == "outcome") {
    # Group negatives by outcome
    calibratedResults <- negatives |>
      dplyr::group_split(.data$outcomeCohortId, .data$analysisId, .keep = TRUE) |>
      furrr::future_map_dfr(.f = function(rows) {
        positives <- metaResults |>
          dplyr::filter(.data$outcomeCohortId == rows$outcomeCohortId[[1]], .data$analysisId == rows$analysisId[[1]])

        negs <- metaResults |>
          dplyr::filter(.data$outcomeCohortId == rows$outcomeCohortId[[1]],
                        .data$analysisId == rows$analysisId[[1]],
                        .data$targetCohortId %in% rows$targetCohortId)
        if (nrow(positives) > 0) {
          calibratedResults <- SelfControlledCohort:::computeCalibratedRows(positives, negs)
          calibratedResults$analysisId <- rows$analysisId[[1]]
          colnames(calibratedResults) <- SqlRender::camelCaseToSnakeCase(colnames(calibratedResults))
          calibratedResults$i2 <- positives$i2
          calibratedResults
        }
      })
  } else {
    # Group negatives by exposure
    calibratedResults <- negatives |>
      dplyr::group_split(.data$targetCohortId, .data$analysisId, .keep = TRUE) |>
      furrr::future_map_dfr(.f = function(rows) {
        positives <- metaResults |>
          dplyr::filter(.data$targetCohortId == rows$targetCohortId[1],
                        .data$analysisId == rows$analysisId[1])

        negs <- metaResults |>
          dplyr::filter(.data$outcomeCohortId %in% rows$outcomeCohortId[1],
                        .data$analysisId == rows$analysisId[1],
                        .data$targetCohortId == rows$targetCohortId[1])

        if (nrow(positives) > 0) {
          calibratedResults <- SelfControlledCohort:::computeCalibratedRows(positives, negs)
          calibratedResults$analysisId <- rows$analysisId[1]
          colnames(calibratedResults) <- SqlRender::camelCaseToSnakeCase(colnames(calibratedResults))
          calibratedResults$i2 <- positives$i2
          calibratedResults
        }
      })
  }

  resultExportManager$exportDataFrame(calibratedResults, "scc_result")
  cli::cli_alert_success("Calibrated meta-analysis complete")
  # upload to db
  SelfControlledCohort::uploadResults(connectionDetails = connectionDetails,
                                      schema = dashboard$config$databaseSchema,
                                      resultsFolder = resultsPath,
                                      forceOverWriteOfSpecifications = TRUE,
                                      purgeSiteDataBeforeUploading = FALSE)

  cli::cli_alert_success("Meta analysis results uploaded")
}
