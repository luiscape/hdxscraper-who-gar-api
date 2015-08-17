#
## LOADS HISTORIC DATA
#  Script to load the current data from HDX
#  into a database so we can merge the new
#  records correctly.
#

library(RCurl)
library(rjson)

#
# Helper scripts.
#
source('scripts/R/helpers/deploy.R')
source(pathDeploy('scripts/R/helpers/write_tables.R'))

#
# Get resource URL from an
# HDX query.
# 
getResourceUrl <- function(resource_id) {

  #
  # Query a CKAN resource ID.
  #
  base_url = 'https://data.hdx.rwlabs.org/api/action/resource_show?id='
  url = paste0(base_url, resource_id)

  #
  # Collect URL from JSON.
  #
  doc <- fromJSON(getURL(url))
  resource_url <- doc$result$url

  #
  # Check that the URL works. 
  #
  if (is.null(resource_url)) stop('Error fetching download URL from HDX. Check resource id.')
  return(resource_url)
}

#
# Download historic data
# and add to database.
#
getHistoricData <- function() {
  url <- getResourceUrl('c59b5722-ca4b-41ca-a446-472d6d824d01')
  temp_f = pathDeploy('data/temp.csv')
  download.file(url, temp_f, method = "wget", quiet = T)
  data <- read.csv(temp_f)
  return(data)
}

#
# Wrapper
#
loadHistoricData <- function() {
  cat('-----------------------------\n')
  cat('Collecting historic data.\n')
  data <- getHistoricData()
  writeTable(data, 'ebola_data_db_format', 'scraperwiki', overwrite = T)
  cat('Done.\n')
  cat('-----------------------------\n')
}

# Calling the function.
loadHistoricData()