\name{sqlsurvey}
\alias{sqlsurvey}
\alias{open.sqlsurvey}
\alias{close.sqlsurvey}
\alias{dim.sqlsurvey}
\alias{open.sqlmodelmatrix}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{Large survey design based on SQL database}
\description{
Specifies a survey data set based on a connection to a SQL (currently MonetDB)
database. The replicate-weights version (\code{\link{sqlrepsurvey}}) provides more functionality.
}
\usage{
sqlsurvey(id, strata = NULL, weights = NULL, fpc = "0", driver, database,
table.name = basename(tempfile("_tbl_")), key ,check.factors=10,...)
\method{close}{sqlsurvey}(con, ...)
\method{open}{sqlsurvey}(con, driver,...)
\method{open}{sqlmodelmatrix}(con, design,...)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{id}{vector of strings with names of sampling unit identifiers}
  \item{strata}{vector of strings with names of stratum identifiers }
  \item{weights}{string with name of weighting variable, or "1" for
    unweighted analysis }
  \item{fpc}{string with name of population size variable (variable may
    be zero for sampling with replacement or infinite population size)}
  \item{driver}{database driver object (from \code{MonetDB})}
  \item{database}{name (URL) of the database containing the data table}
  \item{table.name}{Name for the data table containing the survey data
    and design variables}
  \item{key}{name of a variable that can be used as a unique key }
  \item{check.factors}{If this is a non-zero number, R will attempt to determine which
    variables in the database table are factors based on having at most
    this many distinct values, and will store information on the levels in
    the survey design object.  This can be slow for a very large
    survey.  \code{check.factors} can also be a zero-row data frame
    with the correct factor levels for the factor variables.}
  \item{con}{survey object or survey model matrix object to be
    disconnected from or reconnected to the database}
  \item{design}{A \code{sqlsurvey} object with an active database
    connection}
  \item{...}{for \code{sqlsurvey} and \code{open.sqlsurvey}, other
    arguments to \code{dbConnect}, such as user,password, and database URL}
}

\value{
  \code{sqlsurvey} returns an object of class \code{sqlsurvey}
  
}
\details{
The \code{check.factors} operation can be slow (eg over an hour for an
American Community Survey dataset with 9 million records and 300
variables). If the survey object is saved with \code{save()}, it can be
reconnected to the database with \code{open}, so that it only needs to
be created once.  The most flexible and fastest approach is usually to create the zero-row
data frame manually from the data documentation: only the columns for
factor variables need to be supplied, as the code will assume other
variables are numeric.

  
\code{close} closes the database connection, first attempting to
garbage-collect any tables corresponding to non-existent R objects.

\code{open} re-opens the database connection.

To avoid storing a password in a script, you can use,eg,
\code{password=readline("Password: ")} to be prompted for a password, or
set up a {.monetdb} file in the home directory.

  }
\seealso{\code{\link{sqlrepsurvey}},\code{\link[RMonetDB]{MonetDB}}}
\examples{
\dontrun{
## assumes data already in database
library(sqlsurvey)
monetdriver<-MonetDB(classPath="/usr/local/monetdb/share/monetdb/lib/monetdb-jdbc-2.4.jar")
sqhanes<-sqlsurvey(id="sdmvpsu",strata="sdmvstra",weights="fouryearwt",key="seqn",table.name="nhanesbp",database="jdbc:monetdb://localhost/ACS",driver=monetdriver,user="monetdb",password="monetdb")

sqhanes

## blood pressure by gender
svymean(~bpxsar+bpxdar,design=sqhanes,se=TRUE,byvar=~riagendr)

## hexbin scatterplot
svyplot(bpxsar~bpxdar, design=sqhanes, style="hex")

## linear model
svylm(bpxsar~bpxdar+ridreth1, design=sqhanes)

close(sqhanes)

sqhanes<-open(sqhanes,driver=monetdriver,user="monetdb",password="monetdb")
opar<-par(mfrow=c(2,2))
svyplot(bpxsar~bpxdar, design=sqhanes, style="subsample",col=ifelse(riagendr==1,"blue","magenta"),pch=19)

close(sqhanes)
}
}

\keyword{survey }

