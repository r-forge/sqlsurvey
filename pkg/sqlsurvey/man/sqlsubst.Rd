\name{sqlsubst}
\alias{sqlsubst}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{Substitute variables into a text string}
\description{
When the string contains a name bracketed on both signs by \%\%,
substitute the element of \code{values} with the same name. If the
element of \code{values} is a vector then concatenate with comma
separators first.
}
\usage{
sqlsubst(strings, values)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{strings}{Vector of character strings }
  \item{values}{A list of vectors of values to substitute}
}

\value{
Substituted vector of strings
}

\examples{
sqlsubst("get \%\%vars\%\% from \%\%table\%\%",
     list(vars=c("name","age","sex"), table="survey_data"))
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{survey}

