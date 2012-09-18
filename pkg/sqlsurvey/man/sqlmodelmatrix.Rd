\name{sqlmodelmatrix}
\alias{sqlmodelmatrix}
\alias{head.sqlmm}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{Create model matrix in a SQL table }
\description{
Given a formula and a \code{\link{sqlsurvey}} design object, create SQL
tables with a model frame and model matrix. 
}
\usage{
sqlmodelmatrix(formula, design, fullrank = TRUE)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{formula}{model formula}
  \item{design}{\code{sqlsurvey} object}
  \item{fullrank}{if \code{FALSE} return a matrix of indicators for any
    factor variables, not a full-rank design matrix }
}
\details{
Currently, no transformations are allowed in the formula, factors will
be coded with treatment contrasts, and interactions are allowed only
between factor variables.

The database tables created by this function will be removed if the R
object is deleted and garbage collected.
}
\value{
An object of class \code{sqlmodelmatrix}
}

\seealso{\code{link{open.sqlmodelmatrix}} for reconnecting saved objects }
\examples{
\dontrun{
mm <- sqlmodelmatrix(api00~stype*comp_imp+ell+api99, sqclus1)
dbGetQuery(sqclus1$conn,
     sqlsubst("select * from \%\%mm\%\% limit 2", list(mm=mm$table)))
head(mm)
dbListTables(sqclus1$conn)
rm(mm)
gc()
dbListTables(sqclus1$conn)
close(sqclus1)
}
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{survey}

