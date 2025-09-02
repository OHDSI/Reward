## Utility functions for saving the cohort definitions from the project files
##
#' Cross platform aut for WebApi
.authWebApi <- function(config = config::get()) {
  params <- list(
    baseUrl = config$webApiUrl,
    authMethod = "windows"
  )
  if (.Platform$OS.type != "windows") {
    params$webApiUsername <- Sys.info()['user']
    if (rstudioapi::isAvailable())
      params$webApiPassword <- rstudioapi::askForSecret("Enter your web api password")
    else
      params$webApiPassword <- getPass::getPass("Enter your web api password: ")
  }
  do.call(ROhdsiWebApi::authorizeWebApi, params)
}


#' Get atlas modified times
#' @description
#' Get the time stamp cohorts have changed on the server
#'
getAtlasModifiedTimes <- function(config = config::get(), cohortIds) {
  .authWebApi(config)
  cohortMetaData <- ROhdsiWebApi::getCohortDefinitionsMetaData(baseUrl = config$webApiUrl) |>
    dplyr::mutate(cohortId = .data$id) |>
    dplyr::mutate(modifiedDate = dplyr::if_else(is.na(.data$modifiedDate), .data$createdDate, .data$modifiedDate)) |>
    dplyr::filter(.data$cohortId %in% cohortIds) |>
    dplyr::select("cohortId", "modifiedDate")

  return(cohortMetaData)
}


getCachedConceptSet <- function(config, conceptSetId) {
  .authWebApi(config)
  dir.create("concept_sets", showWarnings = FALSE, recursive = TRUE)
  metaData <- ROhdsiWebApi::getConceptSetDefinitionsMetaData(baseUrl = config$webApiUrl)

  if (!conceptSetId %in% metaData$id)
    cli::cli_abort("{conceptSetId} not found in ATLAS, was it deleted?")

  metaTime <- metaData |>
    dplyr::filter(id == conceptSetId) |>
    dplyr::mutate(modifiedDate = dplyr::if_else(is.na(.data$modifiedDate), .data$createdDate, .data$modifiedDate)) |>
    dplyr::pull("modifiedDate")

  modTime <- getConceptLastUpdateDt(conceptSetId)
  filep <- file.path("concept_sets", paste0(conceptSetId, ".tsv"))
  if (metaTime > modTime) {
    # download, resolve and save concept set json
    csRaw <- ROhdsiWebApi::getConceptSetDefinition(baseUrl = config$webApiUrl, conceptSetId = conceptSetId)
    csIds <- ROhdsiWebApi::resolveConceptSet(csRaw, baseUrl = config$webApiUrl)
    conceptSet <- data.frame(conceptId = csIds)
    readr::write_tsv(conceptSet, filep)
  }

  conceptSet <- readr::read_tsv(filep)
  return(conceptSet)
}

getCohortDefinitionSet <- function() {

  if (!file.exists(file.path("cohorts", "cohorts.csv")))
    return(CohortGenerator::createEmptyCohortDefinitionSet())

  return(
    CohortGenerator::getCohortDefinitionSet(
      settingsFileName = file.path("cohorts", "cohorts.csv"),
      jsonFolder = file.path("cohorts", "json"),
      sqlFolder = file.path("cohorts", "sql"),
      subsetJsonFolder = file.path("cohorts", "subsets"),
      templateFolder = file.path("cohorts", "templates")
    )
  )
}

#' Timestamp of file modified
getLastUpdateDt <- function(cohortId) {
  filep <- file.path("cohorts", "json", paste0(cohortId, ".json"))
  if (!all(file.exists(filep)))
    return(lubridate::origin)

  finfo <- fs::file_info(filep)
  dt <- finfo$modification_time
  return(dt)
}

getConceptLastUpdateDt <- function(conceptSetId) {
  filep <- file.path("concept_sets", paste0(conceptSetId, ".tsv"))
  if (!all(file.exists(filep)))
    return(lubridate::origin)

  finfo <- fs::file_info(filep)
  dt <- finfo$modification_time
  return(dt)
}

addTemplateDefinitions <- function(cohortDefinitionSet) {

  connectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = config::get("dbms"),
    user = config::get("user"),
    password = config::get("password"),
    connectionString = config::get("connectionString")
  )

  executionSettings <- config::get()
  # connect to database
  connection <- DatabaseConnector::connect(connectionDetails)

  on.exit({ DatabaseConnector::disconnect(connection) })
  # Template definitions can't be saved and must be loaded as code.
  rxNormTemplate <- CohortGenerator::createRxNormCohortTemplateDefinition(
    connection = connection,
    cdmDatabaseSchema = executionSettings$vocabularyDatabaseSchema,
    cohortDatabaseSchema = executionSettings$workDatabaseSchema)

  atcTemplateMergedEras <- CohortGenerator::createAtcCohortTemplateDefinition(
    connection = connection,
    cdmDatabaseSchema = executionSettings$vocabularyDatabaseSchema,
    cohortDatabaseSchema = executionSettings$workDatabaseSchema,
    mergeIngredientEras = TRUE,
    priorObservationPeriod = 365
  )

  atcTemplateUnmergedEras <- CohortGenerator::createAtcCohortTemplateDefinition(
    connection = connection,
    identifierExpression = "concept_id * 1000 + 5",
    cdmDatabaseSchema = executionSettings$vocabularyDatabaseSchema,
    cohortDatabaseSchema = executionSettings$workDatabaseSchema,
    mergeIngredientEras = FALSE,
    priorObservationPeriod = 365
  )

  rbind(rxNormTemplate$references,
        atcTemplateMergedEras$refeneces,
        atcTemplateUnmergedEras$references) |>
    dplyr::select("cohortId") |>
    readr::write_csv("cohorts/tpl_exposure_ids.csv")

  snomedTemplate <- CohortGenerator::createSnomedCohortTemplateDefinition(
    connection = connection,
    cdmDatabaseSchema = executionSettings$vocabularyDatabaseSchema,
    cohortDatabaseSchema = executionSettings$workDatabaseSchema,
    requireSecondDiagnosis = FALSE,
    nameSuffix = " (1dx)"
  )

  snomedTemplate$references |>
    dplyr::select("cohortId") |>
    readr::write_csv("cohorts/tpl_outcome_ids.csv")

  cohortDefinitionSet <- cohortDefinitionSet |>
    CohortGenerator::addCohortTemplateDefintion(rxNormTemplate) |>
    CohortGenerator::addCohortTemplateDefintion(atcTemplateMergedEras) |>
    CohortGenerator::addCohortTemplateDefintion(atcTemplateUnmergedEras)|>
    CohortGenerator::addCohortTemplateDefintion(snomedTemplate)

  return(cohortDefinitionSet)
}

#' Use this function to refresh the library of cohorts in the project.
#' If any cohorts have changed since the last time this was run, they should redownload from atlas and re-generate
importRewardCohorts <- function(config = config::get(), recache = FALSE) {
  cohortDefinitionSet <- getCohortDefinitionSet()

  cohortIds <- read.csv(file.path("cohorts", "rewardAtlasCohorts.csv")) |>
    dplyr::bind_rows(readr::read_csv("cohorts/phenotypeLibraryOutcomesIndications.csv", show_col_types = FALSE)) |>
    dplyr::bind_rows(readr::read_csv("cohorts/phenotypeLibraryExposures.csv", show_col_types = FALSE)) |>
    dplyr::pull("cohortId") |>
    unique()

  modTimes <- getAtlasModifiedTimes(config = config, cohortIds = cohortIds)

  if (nrow(cohortDefinitionSet) > 0) {
    if (!"isSubset" %in% colnames(cohortDefinitionSet))
      cohortDefinitionSet$isSubset <- FALSE

    if (!"isTemplatedCohort" %in% colnames(cohortDefinitionSet))
      cohortDefinitionSet$isTemplatedCohort <- FALSE

    cohortsToUpdate <- cohortDefinitionSet |>
      dplyr::filter(!.data$isSubset, !.data$isTemplatedCohort) |>
      dplyr::mutate(cacheDateTime = getLastUpdateDt(.data$cohortId)) |>
      dplyr::left_join(modTimes, by = dplyr::join_by("cohortId"))|>
      dplyr::mutate(modifiedDate = dplyr::if_else(is.na(.data$modifiedDate),
                                                  lubridate::make_datetime(),
                                                  .data$modifiedDate))|>
      dplyr::filter(any(.data$modifiedDate > .data$cacheDateTime,
                        !.data$cohortId %in% cohortDefinitionSet$cohortId))

    if (any(!cohortIds %in% cohortDefinitionSet$cohortId)) {
      cohortsToUpdate <- dplyr::bind_rows(cohortsToUpdate,
                                          data.frame(cohortId = cohortIds[!cohortIds %in% cohortDefinitionSet$cohortId]))
    }

  } else {
    cohortsToUpdate <- modTimes
  }

  if (nrow(cohortsToUpdate) > 0) {
    cli::cli_alert_info("At least one cohort been updated in atlas since last check. Re-Caching")
    .authWebApi()
    updateCohortDefinitionSet <- ROhdsiWebApi::exportCohortDefinitionSet(
      baseUrl = config$webApiUrl,
      cohortIds = cohortsToUpdate$cohortId,
      generateStats = TRUE
    )
    updateCohortDefinitionSet$isTemplatedCohort <- FALSE
    updateCohortDefinitionSet$isSubset <- FALSE

    cohortDefinitionSet <- cohortDefinitionSet |>
      dplyr::filter(!.data$cohortId %in% updateCohortDefinitionSet$cohortId)

    cohortDefinitionSet <- dplyr::bind_rows(cohortDefinitionSet, updateCohortDefinitionSet)

    attr(cohortDefinitionSet, "templateCohortDefinitions") <- NULL
    cohortDefinitionSet <- cohortDefinitionSet |>
      dplyr::mutate(dplyr::if_else(is.na(.data$isSubset),
                                   FALSE,
                                   .data$isSubset)
      ) |>
      dplyr::mutate(dplyr::if_else(is.na(.data$isTemplatedCohort),
                                   FALSE,
                                   .data$isTemplatedCohort)
      ) |>
      dplyr::filter(!.data$isTemplatedCohort)

    cohortDefinitionSet <- addTemplateDefinitions(cohortDefinitionSet)
  }


  cohortDefinitionSet <- cohortDefinitionSet |>
    dplyr::select("cohortId", "cohortName", "sql", "json", "isSubset", "isTemplatedCohort")

  CohortGenerator::saveCohortDefinitionSet(
    cohortDefinitionSet,
    settingsFileName = file.path("cohorts", "cohorts.csv"),
    jsonFolder = file.path("cohorts", "json"),
    sqlFolder = file.path("cohorts", "sql"),
    subsetJsonFolder = file.path("cohorts", "subsets"),
    templateFolder = file.path("cohorts", "templates")
  )

  invisible()
}

getExposureCohortIds <- function() {
  readr::read_csv(file.path("cohorts", "rewardAtlasCohorts.csv"), show_col_types = FALSE) |>
    dplyr::filter(.data$isExposure == 1) |>
    dplyr::select("cohortId") |>
    dplyr::bind_rows(readr::read_csv("cohorts/tpl_exposure_ids.csv", show_col_types = FALSE)) |>
    dplyr::bind_rows(readr::read_csv("cohorts/phenotypeLibraryExposures.csv", show_col_types = FALSE)) |>
    dplyr::pull("cohortId") |>
    unique()
}

getOutcomeCohortIds <- function() {
  readr::read_csv(file.path("cohorts", "rewardAtlasCohorts.csv"), show_col_types = FALSE) |>
    dplyr::filter(.data$isExposure == 0) |>
    dplyr::select("cohortId") |>
    dplyr::bind_rows(readr::read_csv("cohorts/tpl_outcome_ids.csv", show_col_types = FALSE)) |>
    dplyr::bind_rows(readr::read_csv("cohorts/phenotypeLibraryOutcomesIndications.csv", show_col_types = FALSE)) |>
    dplyr::pull("cohortId") |>
    unique()
}


getConceptSetChecksum <- function(conceptSet) {
  checksum <- conceptSet |>
    dplyr::select("conceptId", "includeDescendants", "includeMapped", "isExcluded") |>
    dplyr::mutate(isExcluded = as.integer(.data$isExcluded),
                  includeDescendants = as.integer(.data$includeDescendants),
                  includeMapped = as.integer(.data$includeMapped)) |>
    dplyr::arrange(.data$conceptId, .data$isExcluded, .data$includeDescendants, .data$includeMapped) |>
    digest::digest(algo = "sha256")

  return(checksum)
}


extractCirceConceptSets <- function(cohortDefinition) {
  conceptSets <- list()
  purrr::map(cohortDefinition$ConceptSets, function(csExpression) {

    conceptSet <- data.frame()
    for (item in csExpression$expression$items) {
      conceptSet <- conceptSet |>
        dplyr::bind_rows(
          data.frame(conceptId = item$concept$CONCEPT_ID,
                     isExcluded = as.integer(item$isExcluded),
                     includeDescendants = as.integer(item$includeDescendants),
                     includeMapped = as.integer(item$includeMapped)))
    }

    conceptSets[[csExpression$name]] <<- conceptSet
  })

  return(conceptSets)
}


extractConceptSets <- function(cohortDefinitionSet = getCohortDefinitionSet(), config = config::get()) {
  # Concept sets mapped to uniqiue hashes
  conceptSets <- fastmap::fastmap()
  # checksum to cohort
  cohortChecksums <- fastmap::fastmap()
  # Used for mapping subset cohorts to checksums
  cohortConceptSetMap <- fastmap::fastmap()
  # storage of cohort names - potentially a many to many map
  conceptSetNames <- fastmap::fastmap()

  cohortDefinitionSet |>
    dplyr::filter(!.data$isSubset) |>
    purrr::pwalk(function(cohortName, cohortId, isSubset, isTemplatedCohort, json, ...) {
      if (isTemplatedCohort) {
        # concept set id is just the concept id
        conceptSet <- data.frame(
          conceptId = as.integer(cohortId / 1000),
          includeMapped = 0,
          isExcluded = 0,
          includeDescendants = 1
        )
        checksum <- getConceptSetChecksum(conceptSet)
        conceptSets$set(checksum, conceptSet)

        currVec <- cohortConceptSetMap$get(checksum)
        currVec <- sort(c(currVec, cohortId))
        cohortConceptSetMap$set(checksum, currVec)

        currVec2 <- cohortChecksums$get(as.character(cohortId))
        currVec2 <- sort(c(currVec2, checksum))
        cohortChecksums$set(as.character(cohortId), currVec2)

        names <- conceptSetNames$get(checksum)
        conceptSetNames$set(checksum, unique(c(names, cohortName)))

      } else {
        # extract codesets from json
        cohortDef <- jsonlite::fromJSON(json, simplifyDataFrame = FALSE)
        codesets <- extractCirceConceptSets(cohortDef)

        for (csname in names(codesets)) {
          if (!nrow(codesets[[csname]]))
            next

          # Add to maps
          conceptSet <- codesets[[csname]] |>
            dplyr::select("conceptId", "includeMapped", "includeDescendants", "isExcluded")

          checksum <- getConceptSetChecksum(conceptSet)
          conceptSets$set(checksum, conceptSet)
          currVec <- cohortConceptSetMap$get(checksum)
          cohortConceptSetMap$set(checksum, sort(c(currVec, cohortId)))

          ccVec <- cohortChecksums$get(as.character(cohortId))
          cohortChecksums$set(as.character(cohortId), sort(c(ccVec, cohortId)))

          names <- conceptSetNames$get(checksum)
          conceptSetNames$set(checksum, unique(c(names, csname)))
        }
      }
    })

  # Add any subsets to the hashlists
  cohortDefinitionSet |>
    dplyr::filter(.data$isSubset) |>
    purrr::pwalk(function(cohortId, subsetParent, ...) {
      checksums <- cohortChecksums$get(as.character(cohortId))

      for (checksum in checksums) {
        currVec <- cohortConceptSetMap$get(checksum)
        currVec <- sort(c(currVec, cohortId))
        cohortConceptSetMap$set(checksum, currVec)
      }
    })

  dir.create(config$conceptSetExportPath, showWarnings = FALSE)
  # export unique concept sets and hashes in RDBMS normalized form
  conceptSetExportPath <- file.path(config$conceptSetExportPath, "cg_concept_set.csv")
  cohortConceptSetExportPath <- file.path(config$conceptSetExportPath, "cg_cohort_concept_set.csv")
  conceptSetNamesExportPath <- file.path(config$conceptSetExportPath, "cg_concept_set_name.csv")

  # remove files
  unlink(conceptSetExportPath)
  unlink(cohortConceptSetExportPath)
  unlink(conceptSetNamesExportPath)

  purrr::walk(conceptSets$keys(), function(key) {
    rows <- conceptSets$get(key)
    rows$conceptSetId <- key
    colnames(rows) <- SqlRender::camelCaseToSnakeCase(colnames(rows))
    readr::write_csv(rows, conceptSetExportPath, append = file.exists(conceptSetExportPath))
  })

  # Export conceptset cohort mapping
  purrr::walk(cohortConceptSetMap$keys(), function(key) {
    cohortIds <- cohortConceptSetMap$get(key)
    rows <- data.frame(conceptSetId = key, cohortDefinitionId = cohortIds)
    colnames(rows) <- SqlRender::camelCaseToSnakeCase(colnames(rows))
    readr::write_csv(rows, cohortConceptSetExportPath, append = file.exists(cohortConceptSetExportPath))
  })

  # Export conceptset name to hash key map
  purrr::walk(conceptSetNames$keys(), function(key) {
    namesVec <- conceptSetNames$get(key)
    rows <- data.frame(conceptSetName = namesVec, conceptSetId = key)
    colnames(rows) <- SqlRender::camelCaseToSnakeCase(colnames(rows))
    readr::write_csv(rows, conceptSetNamesExportPath, append = file.exists(conceptSetNamesExportPath))
  })
}

uploadConceptSets <- function(config = config::get()) {
  # Note - quick hack until cohort generator branch is properly implemented
  connection <- DatabaseConnector::connect(getResultsConnectionDetails(config::get()))
  on.exit(DatabaseConnector::disconnect(connection))

  sql <- "
  DROP TABLE IF EXISTS @database_schema.cg_concept_set_name;
  CREATE TABLE @database_schema.cg_concept_set_name(
    concept_set_name varchar,
    concept_set_id varchar(256)
    --PRIMARY KEY (concept_set_id, concept_set_name)
  );

  DROP TABLE IF EXISTS @database_schema.cg_concept_set;
  CREATE TABLE @database_schema.cg_concept_set(
      concept_id bigint,
      include_mapped int,
      include_descendants int,
      is_excluded int,
      concept_set_id varchar(256)
  );

  DROP TABLE IF EXISTS @database_schema.cg_cohort_concept_set;
  CREATE TABLE @database_schema.cg_cohort_concept_set(
      concept_set_id varchar (256),
      cohort_definition_id bigint
      --PRIMARY KEY (concept_set_id, cohort_definition_id)
  );
    "

  DatabaseConnector::renderTranslateExecuteSql(connection = connection, sql = sql, database_schema = config$resultsDatabaseSchema)
  ResultModelManager::pyUploadCsv(connection = connection, table = "cg_concept_set", filepath = file.path(config$conceptSetExportPath, "cg_concept_set.csv"), schema = config$resultsDatabaseSchema)
  ResultModelManager::pyUploadCsv(connection = connection, table = "cg_concept_set_name", filepath = file.path(config$conceptSetExportPath, "cg_concept_set_name.csv"), schema = config$resultsDatabaseSchema)
  ResultModelManager::pyUploadCsv(connection = connection, table = "cg_cohort_concept_set", filepath = file.path(config$conceptSetExportPath, "cg_cohort_concept_set.csv"), schema = config$resultsDatabaseSchema)
}
