\name{sqlvar}
\alias{sqlvar}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{Horvitz-Thompson estimator for multistage sampling}
\description{
Computes the estimated variance matrix for a population total estimate
under stratified multistage cluster sampling with or without
replacement. The variables must be in a separate table from the main
survey data table; they will typically be estimating functions. The
table must have the same key variable as the main data table, which will
be used for an inner join.
}
\usage{
sqlvar(U, utable, design)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{U}{Vector of strings for variable names }
  \item{utable}{String with name of data table for variables}
  \item{design}{\code{sqlsurvey} object }
}

\value{
  A matrix
}
\seealso{\code{\link{sqlsurvey}} }
\examples{

}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{survey }
