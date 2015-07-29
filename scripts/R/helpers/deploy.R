#
# Deploy helper function.
#
# Author: Luis Capelo | capelo@un.org
#

#
# Collects deploy flag from
# command line. This assumes that
# the first argument of scripts
# is always a boolean intended
# for deploy purposes.
#
args <- commandArgs(TRUE)

pathDeploy <- function(f = NULL, l = 'tool/', d = args[1]) {
  if (is.null(args[1])) d = FALSE
  if (d) return(paste0(l,f))
  else return(f)
}