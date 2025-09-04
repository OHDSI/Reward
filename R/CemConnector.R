#' Get Connection to CEM API
#' @description used for exploring evidence
#' @export
#' @family {NegativeControls}
#' @param config assure configuration
getCemConnection <- function(config = config::get()) {
  tryCatch(
    conn <- do.call(CemConnector::createCemConnection, config$cemConnectionDetails),
    error = function(err) cli::cli_abort("unable to connect to Cem API")
  )
  return(conn)
}

#' Get negative control pairs for a given dashboard config
#' @description used for exploring evidence
#' @export
#' @family {NegativeControls}
#' @param config assure configuration
getNegativeControlPairs <- function(config = config::get(), dashboard) {
  # check if dashboard is exposure or outcome controlled
  # Get negative control exposure/outcome pairs
  cemConn <- getCemConnection(config = config)
  on.exit(cemConn$finalize())
  # list cohort concepts for each cohort

  pairs <- data.frame()
  purrr::map(dashboard$config$cohortConceptIds, function(cohortConcept) {
    # Control conceptset is manually defined - don't use cem, use atlas
    if (!is.null(cohortConcept$controlConceptSet)) {
      concepts <- getCachedConceptSet(conceptSetId = cohortConcept$controlConceptSet, config = config)
    } else {
      conceptSet <- data.frame(conceptId = cohortConcept$conceptIds)
      if (dashboard$config$dashboardType == "exposure") {
        concepts <- cemConn$getSuggestedControlCondtions(conceptSet)
      } else {
        concepts <- cemConn$getSuggestedControlIngredients(conceptSet)
      }
    }
    if (nrow(concepts) == 0)
      cli::cli_alert_warning("{cohortConcept$cohortId} has no mapped controls")

    purrr::map(concepts$conceptId * 1000, function(controlCohortId) {
      eop <- data.frame(exposureId = ifelse(dashboard$config$dashboardType == "exposure", cohortConcept$cohortId, controlCohortId),
                        outcomeId = ifelse(dashboard$config$dashboardType == "outcome", cohortConcept$cohortId, controlCohortId),
                        trueEffectSize = 1)
      pairs <<- rbind(pairs, eop)
    })
  })

  # non-custom cohorts use as.integer(cohortId/1000) for concept
  purrr::map(dashboard$config$targetConceptIds, function(conceptId) {
    conceptSet <- data.frame(conceptId = conceptId)
    if (dashboard$config$dashboardType == "exposure") {
      concepts <- cemConn$getSuggestedControlCondtions(conceptSet)
    } else {
      concepts <- cemConn$getSuggestedControlIngredients(conceptSet)
    }
    if (nrow(concepts) == 0)
      cli::cli_alert_warning("Concept - {conceptId} has no mapped controls")
    # List of pairs
    purrr::map2(conceptId * 1000, concepts$conceptId * 1000, function(x, y) {
      eop <- data.frame(exposureId = ifelse(dashboard$config$dashboardType == "exposure", x, y),
                        outcomeId = ifelse(dashboard$config$dashboardType == "outcome", x, y),
                        trueEffectSize = 1)
      pairs <<- rbind(pairs, eop)
    })
  })

  return(pairs)
}