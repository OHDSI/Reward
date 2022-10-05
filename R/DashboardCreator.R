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


#' Create stand-alone dashboaard database from reward data
#' @details
#' Creates and sqlite database from a set of configuration parameters that can be used with the Reward
#' Shiny application.
#'
#' @param configPath            Path to global configuration path
#' @param dashboardConfig       Dashboard configuration options (see create dashboard configuration file)
createDashboardDatabase <- function(configPath,
                                    dashboardConfig,
                                    overwrite = FALSE) {

  ## 1. Create schema/tables in dashboard

  ## 2. Copy relevant data in to dashboard schema

  ## 2 a. use copy if in existing database

  ## 2 b. Import data if in separate database

  ## 3. Add negative control outcomes for exposures within study - mark for hidden or use different table

  ## 4. (optional) Add negative control exposure for outcomes within study - mark for hidden or use different table

  ## 5. Produce calibrated effect estimates


}