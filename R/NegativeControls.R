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

#' Map Negative Controls
#' @description
#' Map negative controls to outcome and exposure cohorts using OHDSI/CemConnector package
#'
#' @import CemConnector
#' @export
mapNegativeControlOutcomes <- function(configPath) {
  dataModel <- RewardDataModel$new(configPath)
  cemConnection <- dataModel$getCemConnection()
  on.exit({
    dataModel$finalize()
    cemConnection$finalize()
  })

  # Get concept sets for all exposure cohorts
  # Use CemConnector to get control outcome concepts
  # This will take a long time with the web api utility
  suggestedControlConditions <- dataModel$getExposureCohortConceptSets() %>%
    dplyr::group_by(cohortDefinitionId) %>%
    dplyr::group_modify(
      ~cemConnection$getSuggestedControlCondtions(.x, nControls = dataModel$config$negativeControlCount)) %>%
    # Map control outcome concepts to cohorts (* 1000)
    dplyr::mutate(outcomeCohortId = conceptId * 1000, outcomeType = 0)

  suggestedControlConditions <- suggestedControlConditions %>%
    dplyr::bind_rows(suggestedControlConditions %>%
                       dplyr::mutate(outcomeCohortId = outcomeCohortId + 1, outcomeType = 1)) %>%
    dplyr::bind_rows(suggestedControlConditions %>%
                       dplyr::mutate(outcomeCohortId = outcomeCohortId + 2, outcomeType = 2))

  suggestedControlConditions
}

#' Map Negative Controls
#' @description
#' Map negative controls to outcome and exposure cohorts using OHDSI/CemConnector package
#'
#' @import CemConnector
#' @export
mapNegativeControlExposures <- function(configPath) {
  dataModel <- RewardDataModel$new(configPath)
  cemConnection <- dataModel$getCemConnection()
  on.exit({
    dataModel$finalize()
    cemConnection$finalize()
  })
  # Get concept sets for all exposure cohorts
  # Use CemConnector to get control outcome concepts
  # This will take a long time with the web api utility
  suggestedControlConditions <- dataModel$getOutcomeCohortConceptSets() %>%
    dplyr::group_by(cohortDefinitionId) %>%
    dplyr::group_modify(
      ~cemConnection$getSuggestedControlExposures(.x, nControls = dataModel$config$negativeControlCount)) %>%
    dplyr::mutate(exposureCohortId = conceptId * 1000)
}
