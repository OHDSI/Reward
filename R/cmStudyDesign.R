#' @importFrom CohortMethod createTruncateIptwArgs
#' @importFrom CohortMethod createMatchOnPsArgs
#' @importFrom CohortMethod createMatchOnPsAndCovariatesArgs
#' @importFrom CohortMethod createStratifyByPsArgs
#' @importFrom CohortMethod createStratifyByPsAndCovariatesArgs
#' @importFrom CohortMethod createComputeCovariateBalanceArgs
#' @importFrom CohortMethod createFitOutcomeModelArgs
#' @importFrom CohortMethod createCmAnalysis
#' @importFrom CohortMethod saveCmAnalysisList
#' @importFrom CohortMethod loadCmAnalysisList
#' @importFrom CohortMethod createOutcome
#' @importFrom CohortMethod createTargetComparatorOutcomes
#' @importFrom CohortMethod saveTargetComparatorOutcomesList
#' @importFrom CohortMethod loadTargetComparatorOutcomesList
#' @importFrom CohortMethod createCmDiagnosticThresholds

#' @noRd
createCohortMethodModuleSpecifications <- function(cmAnalysisList,
                                                   targetComparatorOutcomesList,
                                                   analysesToExclude = NULL,
                                                   refitPsForEveryOutcome = FALSE,
                                                   refitPsForEveryStudyPopulation = TRUE,
                                                   cmDiagnosticThresholds = createCmDiagnosticThresholds()) {
  analysis <- list()
  for (name in names(formals(createCohortMethodModuleSpecifications))) {
    analysis[[name]] <- get(name)
  }

  specifications <- list(module = "CohortMethodModule",
                         version = "0.x.0",
                         remoteRepo = "github.com",
                         remoteUsername = "ohdsi",
                         settings = analysis)
  class(specifications) <- c("CohortMethodModuleSpecifications", "ModuleSpecifications")
  return(specifications)
}

#' @noRd
createCmDesign <- function(targetId,
                           comparatorId,
                           indicationId,
                           outcomeCohortIds,
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
  getDbCohortMethodDataArgs <- createGetDbCohortMethodDataArgs(
    restrictToCommonPeriod = TRUE,
    studyStartDate = studyStartDate,
    studyEndDate = studyEndDate,
    maxCohortSize = 0,
    covariateSettings = covariateSettings
  )
  createPsArgs = createCreatePsArgs(
    maxCohortSizeForFitting = 250000,
    errorOnHighCorrelation = TRUE,
    stopOnError = FALSE, # Setting to FALSE to allow Strategus complete all CM operations; when we cannot fit a model, the equipoise diagnostic should fail
    estimator = "att",
    prior = createPrior(
      priorType = "laplace",
      exclude = c(0),
      useCrossValidation = TRUE
    ),
    control = createControl(
      noiseLevel = "silent",
      cvType = "auto",
      seed = 1,
      resetCoefficients = TRUE,
      tolerance = 2e-07,
      cvRepetitions = 1,
      startingVariance = 0.01
    )
  )
  matchOnPsArgs = createMatchOnPsArgs(
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
  computeSharedCovariateBalanceArgs = createComputeCovariateBalanceArgs(
    maxCohortSize = 250000,
    covariateFilter = NULL
  )
  computeCovariateBalanceArgs = createComputeCovariateBalanceArgs(
    maxCohortSize = 250000,
    covariateFilter = FeatureExtraction::getDefaultTable1Specifications()
  )
  fitOutcomeModelArgs = createFitOutcomeModelArgs(
    modelType = "cox",
    stratified = psMatchMaxRatio != 1,
    useCovariates = FALSE,
    inversePtWeighting = FALSE,
    prior = createPrior(
      priorType = "laplace",
      useCrossValidation = TRUE
    ),
    control = createControl(
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
    createStudyPopArgs <- createCreateStudyPopulationArgs(
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
    cmAnalysisList[[i]] <- createCmAnalysis(
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
    cmDiagnosticThresholds = createCmDiagnosticThresholds(
      mdrrThreshold = Inf,
      easeThreshold = 0.25,
      sdmThreshold = 0.1,
      equipoiseThreshold = 0.2,
      attritionFractionThreshold = 1
    )
  )


  return(cohortMethodModuleSpecifications)
}

