% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/DatabaseSchema.R
\name{insertAtlasCohortRef}
\alias{insertAtlasCohortRef}
\title{Insert atlas cohort ref to postgres db}
\usage{
insertAtlasCohortRef(
  connection,
  config,
  atlasId,
  webApiUrl = NULL,
  cohortDefinition,
  sqlDefinition,
  exposure = FALSE
)
}
\arguments{
\item{connection}{DatabaseConnector::connection}

\item{config}{rewardb global config}

\item{atlasId}{id to atlas cohort to pull down}

\item{exposure}{If exposure, cohort is treated as an exposure, drug domains are captured}
}
\description{
Adds atlas cohort to db reference, from web api
Inserts name/id in to custom cohort table
Maps condition and drug concepts of interest, any desecdants or if they're excluded from the cohort
Concepts not from drug domain are ignored for exposures, concepts not from condition domain are ignored for outcomes
}
