\name{JDBC}
\alias{MonetDB}
\title{
  JDBC engine for MonetDB
}
\description{
  \code{MonetDB} creates a new DBI driver that can be used to start JDBC connections to MonetDB.
}
\usage{
  MonetDB ( classPath = "")
}
\arguments{
  \item{classPath}{The path to the JAR file
    containing the MonetDB driver.}
 }
\value{
  Returns a driver object that can be used in calls to
  \code{\link[DBI]{dbConnect}}.
}
\details{
  \code{RMonetDB} function has two purposes. One is to initialize the Java VM
  and load a Java JDBC driver. The
  second purpose is to create a proxy R object which can be used to a
  call \code{\link[DBI]{dbConnect}} which actually creates a connection.

}
\note{
MonetDB requires \code{CREATE} and \code{DROP} statements to be sent by \code{dbSendUpdate} rather than \code{dbGetQuery}.
}
\seealso{
  \code{\link[DBI]{dbConnect}}
}
\examples{
\dontrun{
monetdriver<-MonetDB(classPath="/usr/local/monetdb/share/monetdb/lib/monetdb-jdbc-2.4.jar")
conn <- dbConnect(drv, "jdbc:monetdb://localhost/demo")
dbListTables(conn)
data(iris)
dbWriteTable(conn, "iris", iris)
dbGetQuery(conn, "select count(*) from iris")
d <- dbReadTable(conn, "iris")
}
}
\keyword{interface}
