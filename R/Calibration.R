# Copyright 2022 Observational Health Data Sciences and Informatics
#
# This file is part of Reward
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Get Null Distribution from EmpiricalCalibration Package
getNullDist <- function(negatives) {
  checkmate::assertDataFrame(negatives, col.names = "named")
  checkmate::assertNames(names(negatives), must.include = c("rr", "seLogRr"))
  negatives <- tidyr::drop_na(negatives)
  EmpiricalCalibration::fitNull(logRr = log(negatives$rr), seLogRr = negatives$seLogRr)
}

#' Create s3 class that matches EmpiricalCalibration's null
createNullDist <- function(mean, sd) {
  null <- c("mean" = mean, "sd" = sd)
  class(null) <- "null"
  return(null)
}

#' @title
#' Compute calibrated rows
#' @description
#' Actual calibration is performed here in a dplyr friendly way
#' @param positives this is the cohort set that should be calibrated
#' @param negatives these are the negative control cohort results
#' @param idCol - either target_cohort_id or outcome_cohort_id, this function is used in p
#' @param calibrationType - value stored in calibrated column of table
#' @return data.frame
computeCalibratedRows <- function(positives,
                                  negatives = NULL,
                                  nullDist = getNullDist(negatives),
                                  idCol = NULL,
                                  keepCols = c("cPt", "cAtRisk", "cCases", "tCases", "tAtRisk"),
                                  calibrationType = 1) {
  checkmate::assertDataFrame(positives, col.names = "named")
  checkmate::assertNames(names(positives),
                         must.include = c("rr", "seLogRr", keepCols, idCol))

  checkmate::assertClass(nullDist, "null")
  errorModel <- EmpiricalCalibration::convertNullToErrorModel(nullDist)
  ci <- EmpiricalCalibration::calibrateConfidenceInterval(log(positives$rr), positives$seLogRr, errorModel)

  # Row matches fields in the database excluding the ids, used in dplyr, group_by with keep_true
  result <- tibble::tibble(calibrated = calibrationType,
                           pValue = EmpiricalCalibration::calibrateP(nullDist, log(positives$rr), positives$seLogRr),
                           ub95 = exp(ci$logUb95Rr),
                           lb95 = exp(ci$logLb95Rr),
                           rr = exp(ci$logRr),
                           seLogRr = ci$seLogRr)

  keptColumns <- positives %>% dplyr::select(dplyr::all_of(c(keepCols, idCol)))
  result <- result %>% dplyr::bind_cols(keptColumns)
  return(result)
}
