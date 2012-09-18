\name{svyquantile.sqlsurvey}
\alias{svyquantile.sqlsurvey}
\alias{svyquantile.sqlrepsurvey}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{Probability-weighted quantiles}
\description{
Estimates quantiles for large surveys.  For surveys with replicate weights, also estimates confidence intervals and standard errors.
}
\usage{
\method{svyquantile}{sqlsurvey}(x, design, quantiles, build.index = FALSE, ...)
\method{svyquantile}{sqlrepsurvey}(x, design, quantiles, se = FALSE, ...)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{x}{Model formula specifying variables }
  \item{design}{\code{sqlsurvey} object }
  \item{quantiles}{Quantiles to be estimated: only a single number is allowed }
\item{se}{Estimate standard errors and confidence intervals}	
  \item{build.index}{Build a temporary index (may increase speed) }
  \item{\dots}{For future expansion}
}

\value{
 A vector of quantiles.
}


\seealso{\code{\link{sqlsurvey}}}
\examples{
\dontrun{
##
svyquantile(~bpxsar, design=sqhanes, quantile=0.5)
svyquantile(~agep, design=acs, quantile=0.5,se=TRUE)
}
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{survey}

