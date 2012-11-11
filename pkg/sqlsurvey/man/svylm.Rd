\name{svylm}
\alias{svylm}
\alias{svylm.sqlsurvey}
\alias{svylm.sqlrepsurvey}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{Linear regression models}
\description{
Fits a linear regression model to a large survey sample.
}
\usage{
svylm(formula, design,...)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{formula}{Model formula. Null models with only an intercept are not supported }
  \item{design}{\code{sqlsurvey} object }
\item{\dots}{dots}
}
\value{
 Vector of regression coefficients with attribute \code{"var"} containing variance matrix.
}
\seealso{ \code{\link{sqlmodelmatrix}}}
\examples{
\dontrun{
svylm(bpxsar~bpxdar+riagendr+ridreth1,design=sqhanes)
}}
\keyword{survey}
