\name{svymean.sqlsurvey}
\alias{svymean.sqlsurvey}
\alias{svymean.sqlrepsurvey}
\alias{svytotal.sqlsurvey}
\alias{svytotal.sqlrepsurvey}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{Means and totals}
\description{
Horvitz-Thompson estimator of means and totals for a large survey. The functions computes cell proportions and counts for factor variables.  For surveys with replicate weights (\code{sqlrepsurvey}) it is possible to use expressions involving mathematical functions, logical operations, \code{\%in\%}, and \code{cut}.
}
\usage{
\method{svymean}{sqlrepsurvey}(x, design, na.rm = TRUE, byvar = NULL, se = TRUE, 
    ...) 
\method{svytotal}{sqlrepsurvey}(x, design, na.rm = TRUE, byvar = NULL, se = TRUE, 
    ...) 
\method{svymean}{sqlsurvey}(x, design, na.rm = TRUE, byvar = NULL,
se = FALSE, keep.estfun = FALSE, ...)
\method{svytotal}{sqlsurvey}(x, design, na.rm = TRUE, byvar = NULL,
se = FALSE, keep.estfun = FALSE, ...)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{x}{model formula }
  \item{design}{\code{sqlsurvey} object }
  \item{na.rm}{Remove missing values (treat as domain)}
  \item{byvar}{Formula with grouping variables}
  \item{se}{Compute variance-covariance matrix?}
  \item{keep.estfun}{Keep the SQL table of estimating functions after
    the standard errors are computed.}
  \item{\dots}{For future expansion }
}
\value{
  A data frame with a column for each variable and a row for each group,
  with the covariance matrix in the \code{"var"} attribute.
}

\seealso{ \code{\link{sqlmodelmatrix}}}
\examples{

}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{survey}

