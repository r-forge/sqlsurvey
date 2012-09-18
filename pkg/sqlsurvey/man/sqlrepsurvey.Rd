\name{sqlrepsurvey}
\alias{sqlrepsurvey}
\alias{open.sqlrepsurvey}
\alias{close.sqlrepsurvey}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Large survey with replicate weights, based on SQL database
}
\description{
Specifies a survey design with replicate weights based on a connection to a relational database (currently MonetDB). 
}
\usage{
sqlrepsurvey(weights, repweights, scale, rscales, driver, database, table.name, key = "row_names", mse = FALSE, check.factors = TRUE, ...)
\method{close}{sqlrepsurvey}(con, ...)
\method{open}{sqlrepsurvey}(con, driver,...)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{weights}{
Character string naming the weight variable
}
  \item{repweights}{
Vector of character strings naming the replicate-weight variables
}
  \item{scale}{
A single number for scaling the sum of squared deviations of the
replicates.
}
  \item{rscales}{
A vector of the same length as \code{repweights} giving per-replicate
scaling for squared deviations of the replicates
}
  \item{driver}{
A database driver object (eg returned by \code{MonetDB()})
}
  \item{database}{
A character string with the name (URL) of a database containing the data table
}
  \item{table.name}{
A character string with the name of data table containing the data and replicate weights
}
  \item{key}{
A character string with the name of a unique identified variable.
}
  \item{mse}{
If \code{TRUE} compute standard errors based on deviations of replicates
from the point estimate, if \code{FALSE} use deviations from the mean of
the replicates.
}
  \item{check.factors}{
If \code{TRUE}, attempt to determine factor levels by querying the
database. This can be slow
}
  \item{\dots}{
Other arguments to \code{dbConnect}, such as user and password
}
\item{con}{object of class \code{sqlrepsurvey}}
}
\details{
  For the American Community Survey, \code{scale} is 4/80 and
  \code{rscales} is \code{rep(1,80)}.
  

The \code{check.factors} operation can be slow (eg over an hour for an
American Community Survey dataset with 9 million records and 300
variables). If the survey object is saved with \code{save()}, it can be
reconnected to the database with \code{open}, so that it only needs to
be created once.

  
\code{close} closes the database connection, first attempting to
garbage-collect any tables corresponding to non-existent R objects.

\code{open} re-opens the database connection.}
\value{
an object of class \code{sqlrepsurvey}
}

\seealso{
\code{\link{sqlsurvey}}, \code{\link[RMonetDB]{MonetDB}}
}
\examples{
\dontrun{
## CRAN policies mean that this package isn't allowed to have examples that run.
## assumes data already in database
library(sqlsurvey)
monetdriver<-MonetDB(classPath="/usr/local/monetdb/share/monetdb/lib/monetdb-jdbc-2.4.jar")

alabama<-sqlrepsurvey("pwgtp",paste("pwgtp",1:80,sep=""),key="idkey",scale=4/80,rscales=rep(1,80),
mse=TRUE,database="jdbc:monetdb://localhost/ACS",
driver=monetdriver,user="monetdb",password="monetdb",table.name="alabama3yr",check.factors=TRUE)

## verify against Census Bureau totals
svytotal(~sex,alabama)
svytotal(~I(agep \%in\% 0:4)+I(agep \%in\% 5:9)+I(agep \%in\% 10:14)+I(agep \%in\% 15:19),alabama)
svytotal(~I(agep \%in\% 20:24)+I(agep \%in\% 25:34)+I(agep \%in\% 35:44)+I(agep \%in\% 45:54),alabama)
svytotal(~I(agep \%in\% 55:59)+I(agep \%in\% 60:64)+I(agep \%in\% 65:74)+I(agep \%in\% 75:84)+I(agep>84),alabama)

## other analyses
svymean(~wagp, subset(alabama, !is.na(wagp)), byvar=~sex,se=TRUE)
svyquantile(~agep, alabama,quantiles=0.5,se=TRUE)

plot(svysmooth(wagp~wkhp,alabama,sample.bandwidth=5000))
}

}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{survey}

