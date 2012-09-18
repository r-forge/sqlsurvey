\name{sqlocpoly}
\alias{svysmooth.sqlsurvey}
\alias{svysmooth.sqlrepsurvey}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{Smoothing and density estimation for large surveys}
\description{
Estimates smooth curves and densities using the binned local polynomial smoothers in the \code{KernSmooth} package.
}
\usage{
\method{svysmooth}{sqlsurvey}(formula, design, bandwidth=NULL, M = 101,sample.bandwidth=NULL,...)
\method{svysmooth}{sqlrepsurvey}(formula, design, bandwidth=NULL, M = 101,sample.bandwidth=NULL,...)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{formula}{two-sided formula (one-sided for density estimation in \code{svysmooth})}
  \item{design}{ \code{sqlsurvey} or\code{sqlrepsurvey} object }

  \item{bandwidth}{ Smoothing bandwidth. Either this or
  \code{sample.bandwidth} must be non-null }
  \item{M}{Number of grid points for binned smoother}	
  \item{sample.bandwidth}{either \code{NULL} or the size of a sample to
    take for estimating bandwidth}	
  \item{\dots}{dots}
}
\details{  
The binning is performed in SQL, so for large data sets the computation
  time is roughly linear in \code{M} and in the number of
  observations. \code{M} should be a few times larger than
  \code{range(x)/bandwidth}, but can often be reduced from the default
  value.

  If \code{sample.bandwidth} is not \code{NULL}, a simple random sample
  of the specified size is taken from the data and the optimal bandwidth is
  computed using the functions in the 'KernSmooth' package and then
 rescaled to the full sample size. 

  Quantile smoothing is not currently available. 
}
\value{
  An object of class \code{svysmooth}, with \code{plot} and \code{lines}
  methods
}

\seealso{\code{\link{sqlsurvey}},\code{\link{sqlrepsurvey}},\code{\link[survey]{svysmooth}} }
\examples{\dontrun{
dens <-svysmooth(~bpxdar, sqhanes, bandwidth=50)
smooth <- svysmooth(bpxsar~bpxdar,subset(sqhanes,bpxdar>40), sample.bandwidth=5000)

dens
plot(dens)
plot(smooth)

}
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{survey}
\keyword{hplot}% __ONLY ONE__ keyword per line
