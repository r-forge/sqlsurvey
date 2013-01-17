\name{sqlhexbin}
\alias{sqlhexbin}
\alias{sqlscatter}
\alias{svyplot.sqlsurvey}
\alias{svyplot.sqlrepsurvey}

%- Also NEED an '\alias' for EACH other topic documented here.
\title{Scatterplots for large surveys}
\description{
Ordinary scatterplots are not useful for large, unequally sampled data.
Two alternatives are hexagonal binning,  in which points are replaced by
hexagons indicating the population weight in regions of the plot, and
thinning, in which one or more iid subsamples taken from the survey sample is
plotted. 
}
\usage{
\method{svyplot}{sqlsurvey}(formula, design, style = c("hex", "grayhex", "subsample"), ...)
\method{svyplot}{sqlrepsurvey}(formula, design, style = c("hex", "grayhex", "subsample"), ...)
sqlhexbin(formula, design, xlab = NULL, ylab = NULL, ..., chunksize =
10000)
sqlscatter(formula, design, npoints = 1000, nplots = 4, ...) 
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{formula}{Formula specifying the two variables to plot }
  \item{design}{\code{sqlsurvey} object }
  \item{xlab}{x-axis label }
  \item{ylab}{y-axis label }
  \item{style}{\code{"hex"} does hexagonal binning, \code{"subsample"}
    does thinned scatterplots}
  \item{npoints}{Number of points to sample}
  \item{nplots}{Number of thinned samples to plot}
  \item{\dots}{Other arguments to \code{gplot.hexbin} or \code{plot} }
  \item{chunksize}{Number of observations to fetch at a time }
}
\details{
Unlike most functions for \code{sqlsurvey} objects, \code{sqlhexbin} fetches all observations for the specified variables, and so would be slow over a network link.
}
\value{
  A object of S4 class \code{hexbin}, invisibly.
}
\references{ Carr, D. B. et al. (1987) Scatterplot Matrix Techniques for Large N. JASA 83, 398, 424-436. }

\seealso{\code{\link{svysmooth.sqlsurvey}}}
\examples{

}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{survey}
\keyword{hplot}% __ONLY ONE__ keyword per line
