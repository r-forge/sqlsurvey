\name{svyloglin.sqlrepsurvey}
\alias{svyloglin.sqlrepsurvey}
\alias{svychisq.sqlrepsurvey}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Loglinear models and tests of independence. 
}
\description{
Fits and compares loglinear models in data from large surveys, including
the Rao-Scott tests in two-way tables as a special case. Currently
available only with replicate weights.  See \code{\link{svyloglin}} for
details on comparing and updating models.
}
\usage{
\method{svyloglin}{sqlrepsurvey}(formula, design, ...)
\method{svychisq}{sqlrepsurvey}(formula, design, pval = c("F", "saddlepoint", "lincom", 
    "chisq"), ...)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{formula}{
One-sided model formula describing the loglinear model (or for
\code{svychisq} the two variables to be tested)
}
  \item{design}{
Object of class \code{sqlrepsurvey}
}
\item{pval}{Specifies the which approximation to use: the default is the
  second-order Rao-Scott correction}
\item{\dots}{
  For expansion
}
}

\value{
An object of class \code{svyloglin}
}
\references{
Rao, JNK, Scott, AJ (1984) "On Chi-squared Tests For Multiway Contingency Tables with Proportions Estimated From Survey Data" Annals of Statistics 12:46-60.
}

\seealso{
\code{\link{svyloglin}}
}
\examples{
\dontrun{
svychisq(~sex+st,design=acs)
}
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{survey}

