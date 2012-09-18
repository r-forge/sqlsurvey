\name{subset.sqlsurvey}
\alias{subset.sqlsurvey}
\alias{subset.sqlrepsurvey}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Subsets of large survey designs
}
\description{
Takes a subset of a large survey design.  This involves creating a new
table containing just the information about which variables are in the
subset.  The new table will be garbage-collected when the R object is no
longer in use. 
}
\usage{
\method{subset}{sqlsurvey}(x, subset, ...)
\method{subset}{sqlrepsurvey}(x, subset, ...)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{x}{
Object of class \code{sqlsurvey} or \code{sqlrepsurvey}.  This will not
be modified in any way
}
  \item{subset}{
R logical expression specifying the subset to keep. The syntax is slightly restricted as the expression must be translatable into SQL: \code{log},\code{exp}, arithmetic,logical, and trigonometric functions, \code{\%in\%}, but not user-defined functions.
}
  \item{\dots}{
for methods
}
}

\value{
object of the same class as \code{x}, specifying the subset. 
}


\examples{
\dontrun{
alabama <- subset(acs, ST==1)

hasbp <- subset(nhanes, !is.na(bpxsar) & !is.na(bpxdar))
}

}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{survey}

