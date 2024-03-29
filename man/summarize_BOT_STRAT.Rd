% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/query.R
\name{summarize_BOT_STRAT}
\alias{summarize_BOT_STRAT}
\title{Chlorophyll trapezoidal integration}
\usage{
summarize_BOT_STRAT(df_data, depth_range = c(0, 100), newvar)
}
\arguments{
\item{depth_range}{vector of 2 values, for chlrophyll integration}

\item{newvar}{should Tmax, depth of Tmin, max cholorphylle be extracted? default to FALSE}

\item{data}{bottles data at discrete depth}
}
\value{
a data frame of 1 row for all variables calculated for the unique identifier
}
\description{
Chlorophyll trapezoidal integration
}
