\name{update.sqlsurvey}
\alias{update.sqlsurvey}
\alias{update.sqlrepsurvey}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Add variables to survey objects
}
\description{
UNDER DEVELOPMENT.  Adds variables to a \code{sqlsurvey} object.  These
are used only by the \code{svymean} and \code{subset} methods at the moment.  Works by defining a
MonetDB function to evaluate the new variable as needed.  At the moment, variables created in one \code{update} call cannot be used in another \code{update} call. This should change eventually.
}
\usage{
\method{update}{sqlsurvey}(object, ...)
\method{update}{sqlrepsurvey}(object, ...)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{object}{
\code{sqlsurvey} object
}
  \item{\dots}{
variables to add, in \code{name=expr} format. 
}
}

\value{
New \code{sqlsurvey} object with added variables.
}


\note{
Functions and operators must exist in MonetDB SQL.  The logical
operators and \code{\%in\%} are translated to their SQL counterparts.

Especially for the \code{sqlsurvey} objects there are some problems with boolean variables, so it's currently better to add zero to a boolean to make it numeric 0/1.

}

%% ~Make other sections like Warning with \section{Warning }{....} ~

\seealso{
\code{\link{sqlsurvey}}
}
\examples{
\dontrun{
## add variables for pulse pressure and hypertension to an NHANES
example object
sqhanes1 <- update(sqhanes, pulsep = bpxsar-bpxdar, htn = 0+(bpxsar>140 | bpxsar>90))

svymean(~bpxsar+bpxdar+pulsep, sqhanes, se=TRUE)
svymean(~bpxsar+bpxdar, subset(sqhanes, htn==1), se=TRUE)

}
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{survey}

