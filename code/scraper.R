## Script to download data from the WHO, 
## parse it, and store it in a database.

# Dependencies
library(RCurl)
# library(rjson)  # may not need it

# SW helper function
onSw <- function(d = T, l = "tool/") {
  if (d) return(l)
  else return("")
}

# Helper functions
source(paste0(onSw(), 'code/write_tables.R'))
source(paste0(onSw(), 'code/sw_status.R'))

# Function to query the WHO API and download
# the file locally.
getWHOFile <- function(date = NULL) {
  # building url using the current date
  if (is.null(date)) date <- Sys.Date()
  date_url <- paste0('http://apps.who.int/gho/athena/xmart/data-coded.csv?target=EBOLA_MEASURE/CASES,DEATHS&filter=LOCATION:-;DATAPACKAGEID:', date, ';INDICATOR_TYPE:SITREP_CUMULATIVE;INDICATOR_TYPE:SITREP_CUMULATIVE_21_DAYS;SEX:-;COUNTRY:GIN;COUNTRY:UNSPECIFIED;COUNTRY:LBR;COUNTRY:UNSPECIFIED;COUNTRY:SLE;COUNTRY:UNSPECIFIED;COUNTRY:GBR;COUNTRY:UNSPECIFIED;COUNTRY:MLI;COUNTRY:UNSPECIFIED;')
  
  # downloading the resulting file in a local
  # temporary csv file
  file_path = paste0(onSw(), 'data/temp.csv')
  download.file(date_url, file_path, method = "wget", quiet = F)
  
  # return message
  return(file_path)
}

# Parse the WHO data into a data.frame that
# follows the format used in HDX.
parseData <- function(custom_date = NULL) {
  
  # downloading file
  file_path = getWHOFile(date = custom_date)
  
  # loading data
  data <- read.csv(file_path)
  
  # checking if there is data at all
  if (nrow(data) == 0) return("Stopping. No new data.")
  
  # cleaning
  data$EPI_DATE <- NULL  # we use the reporting date
  data$COUNTRY <- as.character(data$COUNTRY)
  data$CASE_DEFINITION <- as.character(data$CASE_DEFINITION)
  data$Display.Value <- NULL
  data$Low <- NULL
  data$High <- NULL
  data$INDICATOR_TYPE <- NULL
  data$Comments <- NULL
  data <- subset(data, data$COUNTRY != "UNSPECIFIED")
  data$CASE_DEFINITION <- ifelse(data$CASE_DEFINITION == "", NA, data$CASE_DEFINITION)
  
  # transforming
  data$COUNTRY <- ifelse(data$COUNTRY == 'MLI', "Mali", data$COUNTRY)
  data$COUNTRY <- ifelse(data$COUNTRY == 'GBR', "United Kingdom", data$COUNTRY)
  data$COUNTRY <- ifelse(data$COUNTRY == 'LBR', "Liberia", data$COUNTRY)
  data$COUNTRY <- ifelse(data$COUNTRY == 'GIN', "Guinea", data$COUNTRY)
  data$COUNTRY <- ifelse(data$COUNTRY == 'SLE', "Sierra Leone", data$COUNTRY)
  
  # create indicators
  createIndicators <- function(df = data) {
    # creating variable
    df$Indicator <- NA
    
    ## Creating conditions for each indicator
    # Cases
    df$Indicator <- ifelse(
      df$EBOLA_MEASURE == 'CASES' & df$CASE_DEFINITION == 'CONFIRMED',
      'Cumulative number of confirmed Ebola cases',
      df$Indicator
    )
    df$Indicator <- ifelse(
      df$EBOLA_MEASURE == 'CASES' & df$CASE_DEFINITION == 'PROBABLE',
      'Cumulative number of probable Ebola cases',
      df$Indicator
    )
    df$Indicator <- ifelse(
      df$EBOLA_MEASURE == 'CASES' & df$CASE_DEFINITION == 'SUSPECTED',
      'Cumulative number of suspected Ebola cases',
      df$Indicator
    )
    df$Indicator <- ifelse(
      df$EBOLA_MEASURE == 'CASES' & df$CASE_DEFINITION == 'CONF_PROB_SUSP',
      'Cumulative number of confirmed, probable and suspected Ebola cases',
      df$Indicator
    )
    
    # Deaths
    df$Indicator <- ifelse(
      df$EBOLA_MEASURE == 'DEATHS' & df$CASE_DEFINITION == 'CONFIRMED',
      'Cumulative number of confirmed Ebola deaths',
      df$Indicator
    )
    df$Indicator <- ifelse(
      df$EBOLA_MEASURE == 'DEATHS' & df$CASE_DEFINITION == 'PROBABLE',
      'Cumulative number of probable Ebola deaths',
      df$Indicator
    )
    df$Indicator <- ifelse(
      df$EBOLA_MEASURE == 'DEATHS' & df$CASE_DEFINITION == 'SUSPECTED',
      'Cumulative number of suspected Ebola deaths',
      df$Indicator
    )
    df$Indicator <- ifelse(
      df$EBOLA_MEASURE == 'DEATHS' & df$CASE_DEFINITION == 'CONF_PROB_SUSP',
      'Cumulative number of confirmed, probable and suspected Ebola deaths',
      df$Indicator
    )
    df$Indicator <- ifelse(
      df$EBOLA_MEASURE == 'DEATHS' & is.na(df$CASE_DEFINITION),  # exception
      'Cumulative number of confirmed, probable and suspected Ebola deaths',
      df$Indicator
    )

    # if the indicator hasn't been identified,
    # use the two columns to build a new one
    df$Indicator <- ifelse(
      is.na(df$Indicator),  # if it's still NA
      paste(df$EBOLA_MEASURE, df$CASE_DEFINITION),
      df$Indicator
    )

    ## Two groups missing:
    # - number of cases in the last 21 days
    # - proportion of new cases of the last 21 days

    df$EBOLA_MEASURE <- NULL
    df$CASE_DEFINITION <- NULL

    return(df)
  }
  
  data <- createIndicators()
  
  # reorganizing the indicators
  # sqlite doesn't match columns
  data <- data.frame(Indicator = data$Indicator,
                     Country = data$COUNTRY,
                     Date = data$DATAPACKAGEID,
                     value = data$Numeric)
  
  # Cleaning the final NA records
  data <- data[!is.na(data$value),]

  return(data)
}

# Scraper wrapper
runScraper <- function() {
  cat('-----------------------------\n')
  cat('Collecting current data.\n')
  data <- parseData('2015-01-08')
  # only write data if it is a data.frame
  if (is.data.frame(data)) {
    writeTable(data, 'who_ebola_case_data', 'scraperwiki')
    m <- paste('Data saved on database.', nrow(data), 'records added.\n')
    cat(m)
  }
  else print(data)
  cat('-----------------------------\n')
}

# Changing the status of SW.
tryCatch(runScraper(),
         error = function(e) {
           cat('Error detected ... sending notification.')
           system('mail -s "WHO Ebola figures failed." luiscape@gmail.com')
           changeSwStatus(type = "error", message = "Scraper failed.")
           { stop("!!") }
         }
)

# If success:
changeSwStatus(type = 'ok')
