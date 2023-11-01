#' @title Create a Cyclops prior object
#'
#' @description
#' \code{createPrior} creates a Cyclops prior object for use with \code{\link{fitCyclopsModel}}.
#'
#' @param priorType     Character: specifies prior distribution.  See below for options
#' @param variance      Numeric: prior distribution variance
#' @param exclude       A vector of numbers or covariateId names to exclude from prior
#' @param graph         Child-to-parent mapping for a hierarchical prior
#' @param neighborhood  A list of first-order neighborhoods for a partially fused prior
#' @param useCrossValidation    Logical: Perform cross-validation to determine prior \code{variance}.
#' @param forceIntercept  Logical: Force intercept coefficient into prior
#'
#' @section Prior types:
#'
#' We specify all priors in terms of their variance parameters.
#' Similar fitting tools for regularized regression often parameterize the Laplace distribution
#' in terms of a rate \code{"lambda"} per observation.
#' See \code{"glmnet"}, for example.
#'
#' variance = 2 * / (nobs * lambda)^2 or lambda = sqrt(2 / variance) / nobs
#'
#' @template elaborateExample
#'
#' @return
#' A Cyclops prior object of class inheriting from \code{"cyclopsPrior"} for use with \code{fitCyclopsModel}.
#'
createPrior <- function(priorType,
                        variance = 1,
                        exclude = c(),
                        graph = NULL,
                        neighborhood = NULL,
                        useCrossValidation = FALSE,
                        forceIntercept = FALSE) {
    validNames = c("none", "laplace","normal", "barupdate", "hierarchical", "jeffreys")
    stopifnot(priorType %in% validNames)
    if (!is.null(exclude)) {
        if (!inherits(exclude, "character") &&
                !inherits(exclude, "numeric") &&
                !inherits(exclude, "integer")
        ) {
            stop(cat("Unable to parse excluded covariates:"), exclude)
        }
    }

    if (length(priorType) != length(variance)) {
        stop("Prior types and variances have a dimensionality mismatch")
    }

    if (all(priorType == "none") && useCrossValidation) {
        stop("Cannot perform cross validation with a flat prior")
    }
    if (any(priorType == "barupdate") && useCrossValidation) {
        stop("Cannot perform cross valudation with BAR updates")
    }
    if (any(priorType == "hierarchical") && missing(graph)) {
        stop("Must provide a graph for a hierarchical prior")
    }
    if (!is.null(neighborhood)) {
        allNames <- unlist(neighborhood)
        if (!inherits(allNames, "character") &&
            !inherits(allNames, "numeric") &&
            !inherits(allNames, "integer")) {
            stop(cat("Unable to parse neighborhood covariates:"), allNames)
        }
    }
    structure(list(priorType = priorType, variance = variance, exclude = exclude,
                   graph = graph,
                   neighborhood = neighborhood,
                   useCrossValidation = useCrossValidation, forceIntercept = forceIntercept),
              class = "cyclopsPrior")
}

#' @title Create a Cyclops control object
#'
#' @description
#' \code{createControl} creates a Cyclops control object for use with \code{\link{fitCyclopsModel}}.
#'
#' @param maxIterations			Integer: maximum iterations of Cyclops to attempt before returning a failed-to-converge error
#' @param tolerance					Numeric: maximum relative change in convergence criterion from successive iterations to achieve convergence
#' @param convergenceType		String: name of convergence criterion to employ (described in more detail below)
#' @param cvType						String: name of cross validation search.
#' 													Option \code{"auto"} selects an auto-search following BBR.
#' 													Option \code{"grid"} selects a grid-search cross validation
#' @param fold							Numeric: Number of random folds to employ in cross validation
#' @param lowerLimit				Numeric: Lower prior variance limit for grid-search
#' @param upperLimit				Numeric: Upper prior variance limit for grid-search
#' @param gridSteps					Numeric: Number of steps in grid-search
#' @param cvRepetitions			Numeric: Number of repetitions of X-fold cross validation
#' @param minCVData					Numeric: Minimum number of data for cross validation
#' @param noiseLevel				String: level of Cyclops screen output (\code{"silent"}, \code{"quiet"}, \code{"noisy"})
#' @param threads               Numeric: Specify number of CPU threads to employ in cross-validation; default = 1 (auto = -1)
#' @param seed                  Numeric: Specify random number generator seed. A null value sets seed via \code{\link{Sys.time}}.
#' @param resetCoefficients     Logical: Reset all coefficients to 0 between model fits under cross-validation
#' @param startingVariance      Numeric: Starting variance for auto-search cross-validation; default = -1 (use estimate based on data)
#' @param useKKTSwindle Logical: Use the Karush-Kuhn-Tucker conditions to limit search
#' @param tuneSwindle    Numeric: Size multiplier for active set
#' @param selectorType  String: name of exchangeable sampling unit.
#'                              Option \code{"byPid"} selects entire strata.
#'                              Option \code{"byRow"} selects single rows.
#'                              If set to \code{"auto"}, \code{"byRow"} will be used for all models except conditional models where
#'                              the average number of rows per stratum is smaller than the number of strata.
#' @param initialBound          Numeric: Starting trust-region size
#' @param maxBoundCount         Numeric: Maximum number of tries to decrease initial trust-region size
#' @param algorithm             String: name of fitting algorithm to employ; default is `ccd`
#'
#' Todo: Describe convegence types
#'
#' @return
#' A Cyclops control object of class inheriting from \code{"cyclopsControl"} for use with \code{\link{fitCyclopsModel}}.
#'
#' @template elaborateExample
#'
#' 
createControl <- function(maxIterations = 1000,
                          tolerance = 1E-6,
                          convergenceType = "gradient",
                          cvType = "auto",
                          fold = 10,
                          lowerLimit = 0.01,
                          upperLimit = 20.0,
                          gridSteps = 10,
                          cvRepetitions = 1,
                          minCVData = 100,
                          noiseLevel = "silent",
                          threads = 1,
                          seed = NULL,
                          resetCoefficients = FALSE,
                          startingVariance = -1,
                          useKKTSwindle = FALSE,
                          tuneSwindle = 10,
                          selectorType = "auto",
                          initialBound = 2.0,
                          maxBoundCount = 5,
                          algorithm = "ccd") {
    validCVNames = c("grid", "auto")
    stopifnot(cvType %in% validCVNames)

    validNLNames = c("silent", "quiet", "noisy")
    stopifnot(noiseLevel %in% validNLNames)
    stopifnot(threads == -1 || threads >= 1)
    stopifnot(startingVariance == -1 || startingVariance > 0)
    stopifnot(selectorType %in% c("auto","byPid", "byRow"))

    validAlgorithmNames = c("ccd", "mm")
    stopifnot(algorithm %in% validAlgorithmNames)

    structure(list(maxIterations = maxIterations,
                   tolerance = tolerance,
                   convergenceType = convergenceType,
                   autoSearch = (cvType == "auto"),
                   fold = fold,
                   lowerLimit = lowerLimit,
                   upperLimit = upperLimit,
                   gridSteps = gridSteps,
                   minCVData = minCVData,
                   cvRepetitions = cvRepetitions,
                   noiseLevel = noiseLevel,
                   threads = threads,
                   seed = seed,
                   resetCoefficients = resetCoefficients,
                   startingVariance = startingVariance,
                   useKKTSwindle = useKKTSwindle,
                   tuneSwindle = tuneSwindle,
                   selectorType = selectorType,
                   initialBound = initialBound,
                   maxBoundCount = maxBoundCount,
                   algorithm = algorithm),
              class = "cyclopsControl")
}


#' Create a parameter object for the function getDbCohortMethodData
#'
#' @details
#' Create an object defining the parameter values.
#'
#' @param studyStartDate  A calendar date specifying the minimum date that a cohort index date can appear. Date format is 'yyyymmdd'.
#' @param studyEndDate  A calendar date specifying the maximum date that a cohort index date can appear. Date format is 'yyyymmdd'. Important: the study end data is also used to truncate risk windows, meaning no outcomes beyond the study end date will be considered.
#' @param firstExposureOnly  Should only the first exposure per subject be included? Note that this is typically done in the createStudyPopulation() function, but can already be done here for efficiency reasons.
#' @param removeDuplicateSubjects  Remove subjects that are in both the target and comparator cohort? See details for allowed values.Note that this is typically done in the createStudyPopulation function, but can already be done here for efficiency reasons.
#' @param restrictToCommonPeriod  Restrict the analysis to the period when both treatments are observed?
#' @param washoutPeriod  The minimum required continuous observation time prior to index date for a person to be included in the cohort. Note that this is typically done in the createStudyPopulation function, but can already be done here for efficiency reasons.
#' @param maxCohortSize  If either the target or the comparator cohort is larger than this number it will be sampled to this size. maxCohortSize = 0 indicates no maximum size.
#' @param covariateSettings  An object of type covariateSettings as created using the FeatureExtraction::createCovariateSettings() function.
#'
#' 
createGetDbCohortMethodDataArgs <- function(studyStartDate = "",
                                            studyEndDate = "",
                                            firstExposureOnly = FALSE,
                                            removeDuplicateSubjects = "keep all",
                                            restrictToCommonPeriod = FALSE,
                                            washoutPeriod = 0,
                                            maxCohortSize = 0,
                                            covariateSettings) {
  analysis <- list()
  for (name in names(formals(createGetDbCohortMethodDataArgs))) {
    analysis[[name]] <- get(name)
  }
  class(analysis) <- "args"
  return(analysis)
}

#' Create a parameter object for the function createStudyPopulation
#'
#' @details
#' Create an object defining the parameter values.
#'
#' @param firstExposureOnly  Should only the first exposure per subject be included?
#' @param restrictToCommonPeriod  Restrict the analysis to the period when both exposures are observed?
#' @param washoutPeriod  The minimum required continuous observation time prior to index date for a person to be included in the cohort.
#' @param removeDuplicateSubjects  Remove subjects that are in both the target and comparator cohort? See details for allowed values.
#' @param removeSubjectsWithPriorOutcome  Remove subjects that have the outcome prior to the risk window start?
#' @param priorOutcomeLookback  How many days should we look back when identifying prior outcomes?
#' @param minDaysAtRisk  The minimum required number of days at risk. Risk windows with fewer days than this number are removed from the analysis.
#' @param maxDaysAtRisk  The maximum allowed number of days at risk. Risk windows that are longer will be truncated to this number of days.
#' @param riskWindowStart  The start of the risk window (in days) relative to the startAnchor.
#' @param startAnchor  The anchor point for the start of the risk window. Can be "cohort start" or "cohort end".
#' @param riskWindowEnd  The end of the risk window (in days) relative to the endAnchor.
#' @param endAnchor  The anchor point for the end of the risk window. Can be "cohort start" or "cohort end".
#' @param censorAtNewRiskWindow  If a subject is in multiple cohorts, should time-at-risk be censored when the new time-at-risk starts to prevent overlap?
#'
#' 
createCreateStudyPopulationArgs <- function(firstExposureOnly = FALSE,
                                            restrictToCommonPeriod = FALSE,
                                            washoutPeriod = 0,
                                            removeDuplicateSubjects = "keep all",
                                            removeSubjectsWithPriorOutcome = TRUE,
                                            priorOutcomeLookback = 99999,
                                            minDaysAtRisk = 1,
                                            maxDaysAtRisk = 99999,
                                            riskWindowStart = 0,
                                            startAnchor = "cohort start",
                                            riskWindowEnd = 0,
                                            endAnchor = "cohort end",
                                            censorAtNewRiskWindow = FALSE) {
  analysis <- list()
  for (name in names(formals(createCreateStudyPopulationArgs))) {
    analysis[[name]] <- get(name)
  }
  class(analysis) <- "args"
  return(analysis)
}

#' Create a parameter object for the function createPs
#'
#' @details
#' Create an object defining the parameter values.
#'
#' @param excludeCovariateIds  Exclude these covariates from the propensity model.
#' @param includeCovariateIds  Include only these covariates in the propensity model.
#' @param maxCohortSizeForFitting  If the target or comparator cohort are larger than this number, they will be downsampled before fitting the propensity model. The model will be used to compute propensity scores for all subjects. The purpose of the sampling is to gain speed. Setting this number to 0 means no downsampling will be applied.
#' @param errorOnHighCorrelation  If true, the function will test each covariate for correlation with the treatment assignment. If any covariate has an unusually high correlation (either positive or negative), this will throw and error.
#' @param stopOnError  If an error occur, should the function stop? Else, the two cohorts will be assumed to be perfectly separable.
#' @param prior  The prior used to fit the model. See Cyclops::createPrior() for details.
#' @param control  The control object used to control the cross-validation used to determine the hyperparameters of the prior (if applicable). See Cyclops::createControl() for details.
#' @param estimator  The type of estimator for the IPTW. Options are estimator = "ate" for the average treatment effect, estimator = "att" for the average treatment effect in the treated, and estimator = "ato" for the average treatment effect in the overlap population.
#'
#' 
createCreatePsArgs <- function(excludeCovariateIds = c(),
                               includeCovariateIds = c(),
                               maxCohortSizeForFitting = 250000,
                               errorOnHighCorrelation = TRUE,
                               stopOnError = TRUE,
                               prior = createPrior("laplace", exclude = c(0), useCrossValidation = TRUE),
                               control = createControl(noiseLevel = "silent", cvType = "auto", seed = 1, resetCoefficients = TRUE, tolerance = 2e-07, cvRepetitions = 10, startingVariance = 0.01),
                               estimator = "att") {
  analysis <- list()
  for (name in names(formals(createCreatePsArgs))) {
    analysis[[name]] <- get(name)
  }
  class(analysis) <- "args"
  return(analysis)
}

#' Create a parameter object for the function trimByPs
#'
#' @details
#' Create an object defining the parameter values.
#'
#' @param trimFraction  This fraction will be removed from each treatment group. In the target group, persons with the highest propensity scores will be removed, in the comparator group person with the lowest scores will be removed.
#'
#' 
createTrimByPsArgs <- function(trimFraction = 0.05) {
  analysis <- list()
  for (name in names(formals(createTrimByPsArgs))) {
    analysis[[name]] <- get(name)
  }
  class(analysis) <- "args"
  return(analysis)
}

#' Create a parameter object for the function trimByPsToEquipoise
#'
#' @details
#' Create an object defining the parameter values.
#'
#' @param bounds  The upper and lower bound on the preference score for keeping persons.
#'
#' 
createTrimByPsToEquipoiseArgs <- function(bounds = c(0.3, 0.7)) {
  analysis <- list()
  for (name in names(formals(createTrimByPsToEquipoiseArgs))) {
    analysis[[name]] <- get(name)
  }
  class(analysis) <- "args"
  return(analysis)
}

#' Create a parameter object for the function trimByIptw
#'
#' @details
#' Create an object defining the parameter values.
#'
#' @param maxWeight  The maximum allowed IPTW.
#'
#' 
createTrimByIptwArgs <- function(maxWeight = 10) {
  analysis <- list()
  for (name in names(formals(createTrimByIptwArgs))) {
    analysis[[name]] <- get(name)
  }
  class(analysis) <- "args"
  return(analysis)
}

#' Create a parameter object for the function truncateIptw
#'
#' @details
#' Create an object defining the parameter values.
#'
#' @param maxWeight  The maximum allowed IPTW.
#'
#' 
createTruncateIptwArgs <- function(maxWeight = 10) {
  analysis <- list()
  for (name in names(formals(createTruncateIptwArgs))) {
    analysis[[name]] <- get(name)
  }
  class(analysis) <- "args"
  return(analysis)
}

#' Create a parameter object for the function matchOnPs
#'
#' @details
#' Create an object defining the parameter values.
#'
#' @param caliper  The caliper for matching. A caliper is the distance which is acceptable for any match. Observations which are outside of the caliper are dropped. A caliper of 0 means no caliper is used.
#' @param caliperScale  The scale on which the caliper is defined. Three scales are supported: caliperScale = 'propensity score', caliperScale = 'standardized', or caliperScale = 'standardized logit'. On the standardized scale, the caliper is interpreted in standard deviations of the propensity score distribution. 'standardized logit' is similar, except that the propensity score is transformed to the logit scale because the PS is more likely to be normally distributed on that scale (Austin, 2011).
#' @param maxRatio  The maximum number of persons in the comparator arm to be matched to each person in the treatment arm. A maxRatio of 0 means no maximum: all comparators will be assigned to a target person.
#' @param allowReverseMatch  Allows n-to-1 matching if target arm is larger
#' @param stratificationColumns  Names or numbers of one or more columns in the data data.frame on which subjects should be stratified prior to matching. No persons will be matched with persons outside of the strata identified by the values in these columns.
#'
#' 
createMatchOnPsArgs <- function(caliper = 0.2,
                                caliperScale = "standardized logit",
                                maxRatio = 1,
                                allowReverseMatch = FALSE,
                                stratificationColumns = c()) {
  analysis <- list()
  for (name in names(formals(createMatchOnPsArgs))) {
    analysis[[name]] <- get(name)
  }
  class(analysis) <- "args"
  return(analysis)
}

#' Create a parameter object for the function matchOnPsAndCovariates
#'
#' @details
#' Create an object defining the parameter values.
#'
#' @param caliper  The caliper for matching. A caliper is the distance which is acceptable for any match. Observations which are outside of the caliper are dropped. A caliper of 0 means no caliper is used.
#' @param caliperScale  The scale on which the caliper is defined. Three scales are supported: caliperScale = 'propensity score', caliperScale = 'standardized', or caliperScale = 'standardized logit'. On the standardized scale, the caliper is interpreted in standard deviations of the propensity score distribution. 'standardized logit' is similar, except that the propensity score is transformed to the logit scale because the PS is more likely to be normally distributed on that scale (Austin, 2011).
#' @param maxRatio  The maximum number of persons in the comparator arm to be matched to each person in the treatment arm. A maxRatio of 0 means no maximum: all comparators will be assigned to a target person.
#' @param allowReverseMatch  Allows n-to-1 matching if target arm is larger
#' @param covariateIds  One or more covariate IDs in the cohortMethodData object on which subjects should be also matched.
#'
#' 
createMatchOnPsAndCovariatesArgs <- function(caliper = 0.2,
                                             caliperScale = "standardized logit",
                                             maxRatio = 1,
                                             allowReverseMatch = FALSE,
                                             covariateIds) {
  analysis <- list()
  for (name in names(formals(createMatchOnPsAndCovariatesArgs))) {
    analysis[[name]] <- get(name)
  }
  class(analysis) <- "args"
  return(analysis)
}

#' Create a parameter object for the function stratifyByPs
#'
#' @details
#' Create an object defining the parameter values.
#'
#' @param numberOfStrata  How many strata? The boundaries of the strata are automatically defined to contain equal numbers of target persons.
#' @param stratificationColumns  Names of one or more columns in the data data.frame on which subjects should also be stratified in addition to stratification on propensity score.
#' @param baseSelection  What is the base selection of subjects where the strata bounds are to be determined? Strata are defined as equally-sized strata inside this selection. Possible values are "all", "target", and "comparator".
#'
#' 
createStratifyByPsArgs <- function(numberOfStrata = 5,
                                   stratificationColumns = c(),
                                   baseSelection = "all") {
  analysis <- list()
  for (name in names(formals(createStratifyByPsArgs))) {
    analysis[[name]] <- get(name)
  }
  class(analysis) <- "args"
  return(analysis)
}

#' Create a parameter object for the function stratifyByPsAndCovariates
#'
#' @details
#' Create an object defining the parameter values.
#'
#' @param numberOfStrata  Into how many strata should the propensity score be divided? The boundaries of the strata are automatically defined to contain equal numbers of target persons.
#' @param baseSelection  What is the base selection of subjects where the strata bounds are to be determined? Strata are defined as equally-sized strata inside this selection. Possible values are "all", "target", and "comparator".
#' @param covariateIds  One or more covariate IDs in the cohortMethodData object on which subjects should also be stratified.
#'
#' 
createStratifyByPsAndCovariatesArgs <- function(numberOfStrata = 5,
                                                baseSelection = "all",
                                                covariateIds) {
  analysis <- list()
  for (name in names(formals(createStratifyByPsAndCovariatesArgs))) {
    analysis[[name]] <- get(name)
  }
  class(analysis) <- "args"
  return(analysis)
}

#' Create a parameter object for the function computeCovariateBalance
#'
#' @details
#' Create an object defining the parameter values.
#'
#' @param subgroupCovariateId  Optional: a covariate ID of a binary covariate that indicates a subgroup of interest. Both the before and after populations will be restricted to this subgroup before computing covariate balance.
#' @param maxCohortSize  If the target or comparator cohort are larger than this number, they will be downsampled before computing covariate balance to save time. Setting this number to 0 means no downsampling will be applied.
#' @param covariateFilter  Determines the covariates for which to compute covariate balance. Either a vector of covariate IDs, or a table 1 specifications object as generated for example using FeatureExtraction::getDefaultTable1Specifications(). If covariateFilter = NULL, balance will be computed for all variables found in the data.
#'
#' 
createComputeCovariateBalanceArgs <- function(subgroupCovariateId = NULL,
                                              maxCohortSize = 250000,
                                              covariateFilter = NULL) {
  analysis <- list()
  for (name in names(formals(createComputeCovariateBalanceArgs))) {
    analysis[[name]] <- get(name)
  }
  class(analysis) <- "args"
  return(analysis)
}

#' Create a parameter object for the function fitOutcomeModel
#'
#' @details
#' Create an object defining the parameter values.
#'
#' @param modelType  The type of outcome model that will be used. Possible values are "logistic", "poisson", or "cox".
#' @param stratified  Should the regression be conditioned on the strata defined in the population object (e.g. by matching or stratifying on propensity scores)?
#' @param useCovariates  Whether to use the covariates in the cohortMethodData object in the outcome model.
#' @param inversePtWeighting  Use inverse probability of treatment weighting (IPTW)
#' @param interactionCovariateIds  An optional vector of covariate IDs to use to estimate interactions with the main treatment effect.
#' @param excludeCovariateIds  Exclude these covariates from the outcome model.
#' @param includeCovariateIds  Include only these covariates in the outcome model.
#' @param profileGrid  A one-dimensional grid of points on the log(relative risk) scale where the likelihood for coefficient of variables is sampled. See details.
#' @param profileBounds  The bounds (on the log relative risk scale) for the adaptive sampling of the likelihood function. See details.
#' @param prior  The prior used to fit the model. See Cyclops::createPrior() for details.
#' @param control  The control object used to control the cross-validation used to determine the hyperparameters of the prior (if applicable). See Cyclops::createControl() for details.
#'
#' 
createFitOutcomeModelArgs <- function(modelType = "logistic",
                                      stratified = FALSE,
                                      useCovariates = FALSE,
                                      inversePtWeighting = FALSE,
                                      interactionCovariateIds = c(),
                                      excludeCovariateIds = c(),
                                      includeCovariateIds = c(),
                                      profileGrid = NULL,
                                      profileBounds = c(log(0.1), log(10)),
                                      prior = createPrior("laplace", useCrossValidation = TRUE),
                                      control = createControl(cvType = "auto", seed = 1, resetCoefficients = TRUE, startingVariance = 0.01, tolerance = 2e-07, cvRepetitions = 10, noiseLevel = "quiet")) {
  analysis <- list()
  for (name in names(formals(createFitOutcomeModelArgs))) {
    analysis[[name]] <- get(name)
  }
  class(analysis) <- "args"
  return(analysis)
}
#' Create a CohortMethod analysis specification
#'
#' @details
#' Create a set of analysis choices, to be used with the [runCmAnalyses()] function.
#'
#' Providing a NULL value for any of the argument applies the corresponding step will not be executed.
#' For example, if `createPsArgs = NULL`, no propensity scores will be computed.
#'
#' @param analysisId                      An integer that will be used later to refer to this specific
#'                                        set of analysis choices.
#' @param description                     A short description of the analysis.
#' @param getDbCohortMethodDataArgs       An object representing the arguments to be used when calling
#'                                        the [getDbCohortMethodData()] function.
#' @param createStudyPopArgs              An object representing the arguments to be used when calling
#'                                        the [createStudyPopulation()] function.
#' @param createPsArgs                    An object representing the arguments to be used when calling
#'                                        the [createPs()] function.
#' @param trimByPsArgs                    An object representing the arguments to be used when calling
#'                                        the [trimByPs()] function.
#' @param trimByPsToEquipoiseArgs         An object representing the arguments to be used when calling
#'                                        the [trimByPsToEquipoise()] function.
#' @param trimByIptwArgs                  An object representing the arguments to be used when calling
#'                                        the [trimByIptw()] function.
#' @param truncateIptwArgs                An object representing the arguments to be used when calling
#'                                        the [truncateIptw()] function.
#' @param matchOnPsArgs                   An object representing the arguments to be used when calling
#'                                        the [matchOnPs()] function.
#' @param matchOnPsAndCovariatesArgs      An object representing the arguments to be used when calling
#'                                        the [matchOnPsAndCovariates()] function.
#' @param stratifyByPsArgs                An object representing the arguments to be used when calling
#'                                        the [stratifyByPs()] function.
#' @param stratifyByPsAndCovariatesArgs   An object representing the arguments to be used when calling
#'                                        the [stratifyByPsAndCovariates()] function.
#' @param computeSharedCovariateBalanceArgs  An object representing the arguments to be used when calling
#'                                          the [computeCovariateBalance()] function per target-comparator-analysis.
#' @param computeCovariateBalanceArgs     An object representing the arguments to be used when calling
#'                                        the [computeCovariateBalance()] function per target-comparator-outcome-analysis.
#' @param fitOutcomeModelArgs             An object representing the arguments to be used when calling
#'                                        the [fitOutcomeModel()] function.
#'
#' 
createCmAnalysis <- function(analysisId = 1,
                             description = "",
                             getDbCohortMethodDataArgs,
                             createStudyPopArgs,
                             createPsArgs = NULL,
                             trimByPsArgs = NULL,
                             trimByPsToEquipoiseArgs = NULL,
                             trimByIptwArgs = NULL,
                             truncateIptwArgs = NULL,
                             matchOnPsArgs = NULL,
                             matchOnPsAndCovariatesArgs = NULL,
                             stratifyByPsArgs = NULL,
                             stratifyByPsAndCovariatesArgs = NULL,
                             computeSharedCovariateBalanceArgs = NULL,
                             computeCovariateBalanceArgs = NULL,
                             fitOutcomeModelArgs = NULL) {
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertInt(analysisId, add = errorMessages)
  checkmate::assertCharacter(description, len = 1, add = errorMessages)
  checkmate::assertClass(getDbCohortMethodDataArgs, "args", add = errorMessages)
  checkmate::assertClass(createStudyPopArgs, "args", add = errorMessages)
  checkmate::assertClass(createPsArgs, "args", null.ok = TRUE, add = errorMessages)
  checkmate::assertClass(trimByPsArgs, "args", null.ok = TRUE, add = errorMessages)
  checkmate::assertClass(trimByPsToEquipoiseArgs, "args", null.ok = TRUE, add = errorMessages)
  checkmate::assertClass(trimByIptwArgs, "args", null.ok = TRUE, add = errorMessages)
  checkmate::assertClass(truncateIptwArgs, "args", null.ok = TRUE, add = errorMessages)
  checkmate::assertClass(matchOnPsArgs, "args", null.ok = TRUE, add = errorMessages)
  checkmate::assertClass(matchOnPsAndCovariatesArgs, "args", null.ok = TRUE, add = errorMessages)
  checkmate::assertClass(stratifyByPsArgs, "args", null.ok = TRUE, add = errorMessages)
  checkmate::assertClass(stratifyByPsAndCovariatesArgs, "args", null.ok = TRUE, add = errorMessages)
  checkmate::assertClass(computeSharedCovariateBalanceArgs, "args", null.ok = TRUE, add = errorMessages)
  checkmate::assertClass(computeCovariateBalanceArgs, "args", null.ok = TRUE, add = errorMessages)
  checkmate::assertClass(fitOutcomeModelArgs, "args", null.ok = TRUE, add = errorMessages)
  checkmate::reportAssertions(collection = errorMessages)

  if ((!is.null(matchOnPsArgs)) +
      (!is.null(matchOnPsAndCovariatesArgs)) +
      (!is.null(stratifyByPsArgs)) +
      (!is.null(stratifyByPsAndCovariatesArgs)) > 1) {
    stop("Need to pick one matching or stratification function")
  }
  if ((!is.null(trimByPsArgs)) +
      (!is.null(trimByPsToEquipoiseArgs)) +
      (!is.null(trimByIptwArgs)) > 1) {
    stop("Need to pick one trimming strategy")
  }
  if (is.null(createPsArgs) && (!is.null(trimByPsArgs) |
                                !is.null(trimByPsToEquipoiseArgs) |
                                !is.null(trimByIptwArgs) |
                                !is.null(truncateIptwArgs) |
                                !is.null(matchOnPsArgs) |
                                !is.null(matchOnPsAndCovariatesArgs) |
                                !is.null(stratifyByPsArgs) |
                                !is.null(stratifyByPsAndCovariatesArgs))) {
    stop("Must create propensity score model to use it for trimming, matching, or stratification")
  }
  if (!is.null(fitOutcomeModelArgs) && fitOutcomeModelArgs$stratified && (is.null(matchOnPsArgs) &
                                                                          is.null(matchOnPsAndCovariatesArgs) &
                                                                          is.null(stratifyByPsArgs) &
                                                                          is.null(stratifyByPsAndCovariatesArgs))) {
    stop("Must create strata by using matching or stratification to fit a stratified outcome model")
  }
  if (!is.null(createPsArgs) && (is.null(computeSharedCovariateBalanceArgs) && is.null(computeCovariateBalanceArgs))) {
    message("Note: Using propensity scores but not computing covariate balance")
  }

  analysis <- list()
  for (name in names(formals(createCmAnalysis))) {
    analysis[[name]] <- get(name)
  }

  class(analysis) <- "cmAnalysis"
  return(analysis)
}

#' Save a list of cmAnalysis to file
#'
#' @description
#' Write a list of objects of type `cmAnalysis` to file. The file is in JSON format.
#'
#' @param cmAnalysisList   The cmAnalysis list to be written to file
#' @param file             The name of the file where the results will be written
#'
#' 
saveCmAnalysisList <- function(cmAnalysisList, file) {
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertList(cmAnalysisList, min.len = 1, add = errorMessages)
  for (i in 1:length(cmAnalysisList)) {
    checkmate::assertClass(cmAnalysisList[[i]], "cmAnalysis", add = errorMessages)
  }
  checkmate::assertCharacter(file, len = 1, add = errorMessages)
  checkmate::reportAssertions(collection = errorMessages)

  ParallelLogger::logTrace("Saving cmAnalysisList to ", file)
  ParallelLogger::saveSettingsToJson(cmAnalysisList, file)
}

#' Load a list of cmAnalysis from file
#'
#' @description
#' Load a list of objects of type `cmAnalysis` from file. The file is in JSON format.
#'
#' @param file   The name of the file
#'
#' @return
#' A list of objects of type `cmAnalysis`.
#'
#' 
loadCmAnalysisList <- function(file) {
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertCharacter(file, len = 1, add = errorMessages)
  checkmate::reportAssertions(collection = errorMessages)

  ParallelLogger::logTrace("Loading cmAnalysisList from ", file)
  return(ParallelLogger::loadSettingsFromJson(file))
}

#' Create outcome definition
#'
#' @param outcomeId                        An integer used to identify the outcome in the outcome cohort table.
#' @param outcomeOfInterest                Is this an outcome of interest? If not, creation of non-essential
#'                                         files will be skipped, including outcome=specific covariate balance
#'                                         files. This could be helpful to speed up analyses with many controls,
#'                                         for which we're only interested in the effect size estimate.
#' @param trueEffectSize                   For negative and positive controls: the known true effect size. To be used
#'                                         for empirical calibration. Negative controls have `trueEffectSize = 1`. If
#'                                         the true effect size is unknown, use `trueEffectSize = NA`
#' @param priorOutcomeLookback             How many days should we look back when identifying prior.
#'                                         outcomes?
#' @param riskWindowStart                  The start of the risk window (in days) relative to the `startAnchor`.
#' @param startAnchor                      The anchor point for the start of the risk window. Can be `"cohort start"`
#'                                         or `"cohort end"`.
#' @param riskWindowEnd                    The end of the risk window (in days) relative to the `endAnchor`.
#' @param endAnchor                        The anchor point for the end of the risk window. Can be `"cohort start"`
#'                                         or `"cohort end"`.
#'
#' @details
#' Any settings here that are not `NULL` will override any values set in [createCreateStudyPopulationArgs()].
#'
#'
#' @return
#' An object of type `outcome`, to be used in [createTargetComparatorOutcomes()].
#'
#' 
createOutcome <- function(outcomeId,
                          outcomeOfInterest = TRUE,
                          trueEffectSize = NA,
                          priorOutcomeLookback = NULL,
                          riskWindowStart = NULL,
                          startAnchor = NULL,
                          riskWindowEnd = NULL,
                          endAnchor = NULL) {
  if (!is.null(startAnchor) && !grepl("start$|end$", startAnchor, ignore.case = TRUE)) {
    stop("startAnchor should have value 'cohort start' or 'cohort end'")
  }
  if (!is.null(riskWindowEnd) && !grepl("start$|end$", endAnchor, ignore.case = TRUE)) {
    stop("endAnchor should have value 'cohort start' or 'cohort end'")
  }

  outcome <- list()
  for (name in names(formals(createOutcome))) {
    outcome[[name]] <- get(name)
  }
  class(outcome) <- "outcome"
  return(outcome)
}

#' Create target-comparator-outcomes combinations.
#'
#' @details
#' Create a set of hypotheses of interest, to be used with the [runCmAnalyses()] function.
#'
#' @param targetId                      A cohort ID identifying the target exposure in the exposure
#'                                      table.
#' @param comparatorId                  A cohort ID identifying the comparator exposure in the exposure
#'                                      table.
#' @param outcomes                      A list of object of type `outcome` as created by
#'                                      [createOutcome()].
#' @param excludedCovariateConceptIds   A list of concept IDs that cannot be used to construct
#'                                      covariates. This argument is to be used only for exclusion
#'                                      concepts that are specific to the target-comparator combination.
#' @param includedCovariateConceptIds   A list of concept IDs that must be used to construct
#'                                      covariates. This argument is to be used only for inclusion
#'                                      concepts that are specific to the target-comparator combination.
#'
#' @return
#' An object of type `targetComparatorOutcomes`.
#'
#' 
createTargetComparatorOutcomes <- function(targetId,
                                           comparatorId,
                                           outcomes,
                                           excludedCovariateConceptIds = c(),
                                           includedCovariateConceptIds = c()) {
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertInt(targetId, add = errorMessages)
  checkmate::assertInt(comparatorId, add = errorMessages)
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

#' Save a list of targetComparatorOutcomes to file
#'
#' @description
#' Write a list of objects of type `targetComparatorOutcomes` to file. The file is in JSON format.
#'
#' @param targetComparatorOutcomesList   The targetComparatorOutcomes list to be written to file
#' @param file                         The name of the file where the results will be written
#'
#' 
saveTargetComparatorOutcomesList <- function(targetComparatorOutcomesList, file) {
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertList(targetComparatorOutcomesList, min.len = 1, add = errorMessages)
  for (i in 1:length(targetComparatorOutcomesList)) {
    checkmate::assertClass(targetComparatorOutcomesList[[i]], "targetComparatorOutcomes", add = errorMessages)
  }
  checkmate::assertCharacter(file, len = 1, add = errorMessages)
  checkmate::reportAssertions(collection = errorMessages)

  ParallelLogger::saveSettingsToJson(targetComparatorOutcomesList, file)
}

#' Load a list of targetComparatorOutcomes from file
#'
#' @description
#' Load a list of objects of type `targetComparatorOutcomes` from file. The file is in JSON format.
#'
#' @param file   The name of the file
#'
#' @return
#' A list of objects of type `targetComparatorOutcomes`.
#'
#' 
loadTargetComparatorOutcomesList <- function(file) {
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertCharacter(file, len = 1, add = errorMessages)
  checkmate::reportAssertions(collection = errorMessages)
  return(ParallelLogger::loadSettingsFromJson(file))
}




createCmDiagnosticThresholds <- function(mdrrThreshold = 10,
                                         easeThreshold = 0.25,
                                         sdmThreshold = 0.1,
                                         equipoiseThreshold = 0.2,
                                         attritionFractionThreshold = 1) {
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertNumeric(mdrrThreshold, len = 1, lower = 0, add = errorMessages)
  checkmate::assertNumeric(easeThreshold, len = 1, lower = 0, add = errorMessages)
  checkmate::assertNumeric(sdmThreshold, len = 1, lower = 0, add = errorMessages)
  checkmate::assertNumeric(equipoiseThreshold, len = 1, lower = 0, add = errorMessages)
  checkmate::assertNumeric(attritionFractionThreshold, len = 1, lower = 0, add = errorMessages)
  checkmate::reportAssertions(collection = errorMessages)
  thresholds <- list()
  for (name in names(formals(createCmDiagnosticThresholds))) {
    thresholds[[name]] <- get(name)
  }
  class(thresholds) <- "CmDiagnosticThresholds"
  return(thresholds)
}


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
                         version = "0.2.0",
                         remoteRepo = "github.com",
                         remoteUsername = "ohdsi",
                         settings = analysis)
  class(specifications) <- c("CohortMethodModuleSpecifications", "ModuleSpecifications")
  return(specifications)
}

createStrategusCmDesign <- function(targetId, comparatorId, indicationId, outcomeCohortIds, excludedCovariateConceptIds) {

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
  negativeConceptSetId <- 257
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
    # Get the subset definition ID that matches
    # the target ID. The comparator will also use the same subset
    # definition ID
    # currentSubsetDefinitionId <- dfUniqueTcis %>%
    #   filter(cohortId == tci$targetId &
    #            indicationId == paste(tci$indicationId, collapse = ",") &
    #            genderConceptIds == paste(tci$genderConceptIds, collapse = ",") &
    #            minAge == paste(tci$minAge, collapse = ",") &
    #            maxAge == paste(tci$maxAge, collapse = ",")) %>%
    #   pull(subsetDefinitionId)
    #
    #
    # targetId <- cohortDefinitionSet %>%
    #   filter(subsetParent == tci$targetId & subsetDefinitionId == currentSubsetDefinitionId) %>%
    #   pull(cohortId)
    # comparatorId <- cohortDefinitionSet %>%
    #   filter(subsetParent == tci$comparatorId & subsetDefinitionId == currentSubsetDefinitionId) %>%
    #   pull(cohortId)

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

