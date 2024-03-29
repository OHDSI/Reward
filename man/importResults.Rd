% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/ImportResults.R
\name{importResults}
\alias{importResults}
\title{Import Results from CDM}
\usage{
importResults(
  config,
  resultsZipPath,
  connection = NULL,
  cleanup = TRUE,
  computeStatsTables = TRUE
)
}
\arguments{
\item{config}{Reward configuration object}

\item{resultsZipPath}{path to exported results}

\item{connection}{DatabaseConnector connection to reward databases}

\item{cleanup}{Remove extracted csv files after inserting results}

\item{computeAggregateTables}{compute tables that count aggregated stats across different data sources}
}
\description{
Verify and import results exported from CDMs
}
