## Loading historic data.
#  Script to load the current data from HDX
#  into a database so we can merge the new
#  records correctly.

# Dependencies
library(RCurl)
library(rjson)

# SW helper function
onSw <- function(d = F, l = 'tool/') {
  if (d) return(l)
  else return("")
}

# Loading other helper functions
source(paste0(onSw(), 'code/write_tables.R'))

# Helper function to get a resource_id
# from CKAN.
getResourceUrl <- function(resource_id) {
  # query ckan for the resource id
  base_url = 'https://data.hdx.rwlabs.org/api/action/resource_show?id='
  url = paste0(base_url, resource_id)
  # navigate and grab url
  doc <- fromJSON(getURL(url))
  resource_url <- doc$result$url
  return(resource_url)
}

# Getting historic data and adding it
# to a database.
getHistoricData <- function() {
  url <- getResourceUrl('f48a3cf9-110e-4892-bedf-d4c1d725a7d1')
  temp_f = paste0(onSw(), 'data/temp.csv')
  download.file(url, temp_f, method = "wget", quiet = T)
  data <- read.csv(temp_f)
  return(data)
}

# Wrapper for everything.
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