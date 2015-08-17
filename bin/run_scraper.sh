#!/bin/bash

#
# A custom date can be passed as an argument to the
# scraper.R script.
# Param 1: Deploy takes TRUE or FALSE
# Param 2: Date takes ISO data as "2015-01-16"
#
# Example:
# ~/R/bin/Rscript ~/tool/code/scraper.R FALSE "2015-01-16"
#
# ~/R/bin/Rscript ~/tool/code/scraper.R TRUE

#
# If a sequence of dates is needed,
# use this.
#
while read p; do
  ~/R/bin/Rscript ~/tool/code/scraper.R FALSE $p
done < ~/tool/data/missed_dates.csv