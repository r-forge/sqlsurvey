\name{monet.read.csv}
\alias{monet.read.csv}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{
Try to read CSV files directly into MonetDB from R.  Can be much faster than reading into R and then saving.
}
\description{
This function reads the initial lines of a CSV file to attempt to work out column classes, uses \code{dbWriteTable} to create an empty table, and then calls the MonetDB bulk input statement to read the file.
}
\usage{
monet.read.csv(connection, files, tablename, nrows, header = TRUE, locked = FALSE, na.strings = "", ..., nrow.check = 500)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{connection}{Connection to a MonetDB database
}
  \item{files}{
Vector of one or more filenames, for files to be put in the same table.
}
  \item{tablename}{
Character string giving the table to be created
}
  \item{nrows}{
Approximate number of rows in the file (to reduce copying)
}
  \item{header}{
as in \code{\link{read.csv}}
}
  \item{locked}{
 If \code{TRUE}, lock the table while loading the data (faster)
}
  \item{na.strings}{
A single character string denoting missing values, to be converted to SQL NULL.
}
  \item{\dots}{
Other arguments to \code{\link{read.csv}}, in particular, \code{colClasses}
}
  \item{nrow.check}{
Number of rows to read while attempting to work out variable types.
}
}

\value{
Called for its side-effect
}

\seealso{
\code{\link[DBI]{dbWriteTable}}
}
\examples{
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{interface}

