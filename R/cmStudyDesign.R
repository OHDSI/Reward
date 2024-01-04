# Remove check int which breaks big int values
createOutcome <- function(outcomeId, outcomeOfInterest = TRUE, trueEffectSize = NA, priorOutcomeLookback = NULL, riskWindowStart = NULL, startAnchor = NULL, riskWindowEnd = NULL, endAnchor = NULL) {
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertNumeric(outcomeId, add = errorMessages)
  checkmate::assertLogical(outcomeOfInterest, add = errorMessages)
  checkmate::assertNumeric(trueEffectSize, len = 1, null.ok = TRUE, add = errorMessages)
  checkmate::assertInt(riskWindowStart, null.ok = TRUE, add = errorMessages)
  checkmate::assertInt(riskWindowEnd, null.ok = TRUE, add = errorMessages)
  checkmate::reportAssertions(collection = errorMessages)
  if (!is.null(startAnchor) && !grepl("start$|end$", startAnchor, ignore.case = TRUE)) {
    stop("startAnchor should have value \'cohort start\' or \'cohort end\'")
  }
  if (!is.null(riskWindowEnd) && !grepl("start$|end$", endAnchor, ignore.case = TRUE)) {
    stop("endAnchor should have value \'cohort start\' or \'cohort end\'")
  }
  outcome <- list()
  for (name in names(formals(createOutcome))) {
    outcome[[name]] <- get(name)
  }
  class(outcome) <- "outcome"
  return(outcome)
}

# Remove check int which breaks big int values
createTargetComparatorOutcomes <- function(targetId, comparatorId, outcomes, excludedCovariateConceptIds = c(), includedCovariateConceptIds = c()) {
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertNumeric(targetId, add = errorMessages)
  checkmate::assertNumeric(comparatorId, add = errorMessages)
  checkmate::assertList(outcomes, min.len = 1, add = errorMessages)
  for (i in seq_along(outcomes)) {
    checkmate::assertClass(outcomes[[i]], "outcome", add = errorMessages)
  }
  checkmate::assertIntegerish(excludedCovariateConceptIds, null.ok = TRUE, add = errorMessages)
  checkmate::assertIntegerish(includedCovariateConceptIds, null.ok = TRUE, add = errorMessages)
  checkmate::reportAssertions(collection = errorMessages)
  outcomeIds <- rep(0, length(outcomes))
  for (i in seq_along(outcomes)) {
    outcomeIds[i] <- outcomes[[i]]$outcomeId
  }
  duplicatedIds <- outcomeIds[duplicated(outcomeIds)]
  if (length(duplicatedIds) > 0) {
    stop(sprintf("Found duplicate outcome IDs: %s", paste(duplicatedIds, paste = ", ")))
  }
  targetComparatorOutcomes <- list()
  for (name in names(formals(createTargetComparatorOutcomes))) {
    targetComparatorOutcomes[[name]] <- get(name)
  }
  class(targetComparatorOutcomes) <- "targetComparatorOutcomes"
  return(targetComparatorOutcomes)
}


#' @noRd
createCohortMethodModuleSpecifications <- function(cmAnalysisList,
                                                   #dataSources,
                                                   targetComparatorOutcomesList,
                                                   analysesToExclude = NULL,
                                                   refitPsForEveryOutcome = FALSE,
                                                   refitPsForEveryStudyPopulation = TRUE,
                                                   cmDiagnosticThresholds = createCmDiagnosticThresholds()) {
  analysis <- list()
  for (name in names(formals(createCohortMethodModuleSpecifications))) {
    analysis[[name]] <- get(name)
  }

  specifications <- list(module = "RewardCohortMethodSettings",
                         version = utils::packageVersion(utils::packageName()),
                         settings = analysis)
  class(specifications) <- c("CohortMethodModuleSpecifications", "ModuleSpecifications")
  return(specifications)
}

#' @noRd
createCmDesign <- function(targetId,
                           comparatorId,
                           indicationId,
                           outcomeCohortIds,
                           dataSources,
                           excludedCovariateConceptIds) {

  tcis <- list(
    #standard analyses that would be performed during routine signal detection
    list(
      targetId = targetId, # e.g New users of ACE inhibitors
      comparatorId = comparatorId, # e.g New users of Alpha-1 Blockers
      indicationId = indicationId, # e.g Hypertension
      genderConceptIds = c(8507, 8532), # use valid genders (remove unknown)
      minAge = NULL, # All ages In years. Can be NULL
      maxAge = NULL, # All ages In years. Can be NULL
      excludedCovariateConceptIds = excludedCovariateConceptIds
    )
  )
  outcomes <- tibble::tibble(
    cohortId = outcomeCohortIds,
    cleanWindow = c(365)
  )

  timeAtRisks <- tibble::tibble(
    label = c("On treatment", "fixed 365d"),
    riskWindowStart = c(1, 1),
    startAnchor = c("cohort start", "cohort start"),
    riskWindowEnd = c(0, 365),
    endAnchor = c("cohort end", "cohort start"),
  )

  studyStartDate <- "20010101" # YYYYMMDD, e.g. "2001-02-01" for January 1st, 2001
  studyEndDate <- "20251231" # YYYYMMDD

  useCleanWindowForPriorOutcomeLookback <- FALSE # If FALSE, lookback window is all time prior, i.e., including only first events
  psMatchMaxRatio <- 1 # If bigger than 1, the outcome model will be conditioned on the matched set

  covariateSettings <- FeatureExtraction::createDefaultCovariateSettings(
    addDescendantsToExclude = TRUE # Keep TRUE because you're excluding concepts
  )
  outcomeList <-
    lapply(seq_len(nrow(outcomes)), function(i) {
      if (useCleanWindowForPriorOutcomeLookback)
        priorOutcomeLookback <- outcomes$cleanWindow[i]
      else
        priorOutcomeLookback <- 99999
      createOutcome(
        outcomeId = outcomes$cohortId[i],
        outcomeOfInterest = TRUE,
        trueEffectSize = NA,
        priorOutcomeLookback = priorOutcomeLookback
      )
    })

  targetComparatorOutcomesList <- list()
  for (i in seq_along(tcis)) {
    tci <- tcis[[i]]
    targetComparatorOutcomesList[[i]] <- createTargetComparatorOutcomes(
      targetId = tci$targetId,
      comparatorId = tci$comparatorId,
      outcomes = outcomeList,
      excludedCovariateConceptIds = tci$excludedCovariateConceptIds
    )
  }
  getDbCohortMethodDataArgs <- CohortMethod::createGetDbCohortMethodDataArgs(
    restrictToCommonPeriod = TRUE,
    studyStartDate = studyStartDate,
    studyEndDate = studyEndDate,
    maxCohortSize = 0,
    covariateSettings = covariateSettings
  )
  createPsArgs = CohortMethod::createCreatePsArgs(
    maxCohortSizeForFitting = 250000,
    errorOnHighCorrelation = TRUE,
    stopOnError = FALSE, # Setting to FALSE to allow Strategus complete all CM operations; when we cannot fit a model, the equipoise diagnostic should fail
    estimator = "att",
    prior = Cyclops::createPrior(
      priorType = "laplace",
      exclude = c(0),
      useCrossValidation = TRUE
    ),
    control = Cyclops::createControl(
      noiseLevel = "silent",
      cvType = "auto",
      seed = 1,
      resetCoefficients = TRUE,
      tolerance = 2e-07,
      cvRepetitions = 1,
      startingVariance = 0.01
    )
  )
  matchOnPsArgs = CohortMethod::createMatchOnPsArgs(
    maxRatio = psMatchMaxRatio,
    caliper = 0.2,
    caliperScale = "standardized logit",
    allowReverseMatch = FALSE,
    stratificationColumns = c()
  )
  # stratifyByPsArgs <- CohortMethod::createStratifyByPsArgs(
  #   numberOfStrata = 5,
  #   stratificationColumns = c(),
  #   baseSelection = "all"
  # )
  computeSharedCovariateBalanceArgs = CohortMethod::createComputeCovariateBalanceArgs(
    maxCohortSize = 250000,
    covariateFilter = NULL
  )
  computeCovariateBalanceArgs = CohortMethod::createComputeCovariateBalanceArgs(
    maxCohortSize = 250000,
    covariateFilter = FeatureExtraction::getDefaultTable1Specifications()
  )
  fitOutcomeModelArgs = CohortMethod::createFitOutcomeModelArgs(
    modelType = "cox",
    stratified = psMatchMaxRatio != 1,
    useCovariates = FALSE,
    inversePtWeighting = FALSE,
    prior = Cyclops::createPrior(
      priorType = "laplace",
      useCrossValidation = TRUE
    ),
    control = Cyclops::createControl(
      cvType = "auto",
      seed = 1,
      resetCoefficients = TRUE,
      startingVariance = 0.01,
      tolerance = 2e-07,
      cvRepetitions = 10,
      noiseLevel = "quiet"
    )
  )
  cmAnalysisList <- list()
  for (i in seq_len(nrow(timeAtRisks))) {
    createStudyPopArgs <- CohortMethod::createCreateStudyPopulationArgs(
      firstExposureOnly = FALSE,
      washoutPeriod = 0,
      removeDuplicateSubjects = "keep first",
      censorAtNewRiskWindow = TRUE,
      removeSubjectsWithPriorOutcome = TRUE,
      priorOutcomeLookback = 99999,
      riskWindowStart = timeAtRisks$riskWindowStart[[i]],
      startAnchor = timeAtRisks$startAnchor[[i]],
      riskWindowEnd = timeAtRisks$riskWindowEnd[[i]],
      endAnchor = timeAtRisks$endAnchor[[i]],
      minDaysAtRisk = 1,
      maxDaysAtRisk = 99999
    )
    cmAnalysisList[[i]] <- CohortMethod::createCmAnalysis(
      analysisId = i,
      description = sprintf(
        "Cohort method, %s",
        timeAtRisks$label[i]
      ),
      getDbCohortMethodDataArgs = getDbCohortMethodDataArgs,
      createStudyPopArgs = createStudyPopArgs,
      createPsArgs = createPsArgs,
      matchOnPsArgs = matchOnPsArgs,
      # stratifyByPsArgs = stratifyByPsArgs,
      computeSharedCovariateBalanceArgs = computeSharedCovariateBalanceArgs,
      computeCovariateBalanceArgs = computeCovariateBalanceArgs,
      fitOutcomeModelArgs = fitOutcomeModelArgs
    )
  }
  cohortMethodModuleSpecifications <- createCohortMethodModuleSpecifications(
    cmAnalysisList = cmAnalysisList,
    targetComparatorOutcomesList = targetComparatorOutcomesList,
    analysesToExclude = NULL,
    refitPsForEveryOutcome = FALSE,
    refitPsForEveryStudyPopulation = FALSE,
    cmDiagnosticThresholds = CohortMethod::createCmDiagnosticThresholds(
      mdrrThreshold = Inf,
      easeThreshold = 0.25,
      sdmThreshold = 0.1,
      equipoiseThreshold = 0.2,
      generalizabilitySdmThreshold = 1
    )
    #,dataSources = dataSources
  )


  return(cohortMethodModuleSpecifications)
}

