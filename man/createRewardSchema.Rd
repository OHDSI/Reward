% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/DatabaseSchema.R
\name{createRewardSchema}
\alias{createRewardSchema}
\title{Build postgres schema}
\usage{
createRewardSchema(
  configFilePath,
  settingsFilePath = system.file("settings", "defaultSccArgs.json", package =
    utils::packageName())
)
}
\arguments{
\item{configFilePath}{path to global reward config}
}
\description{
Build the reward database schema for postgres instance from scratch
This will also require a CEM schema to be built which uses the OHDSI Common Evidence Model to generate the matrix
of known assocations for OMOP Standard Vocabulary terms. This is required for generating any stats that require negative controls
}
