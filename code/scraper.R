## Script to download data from the WHO, 
## parse it, and store it in a database.

# Configuration: passing args via command line.
args <- commandArgs(T)

# Dependencies
library(RCurl)
library(sqldf)

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
  if (is.na(date)) date <- Sys.Date()  # if command line argument is not provided
  date_url <- paste0('http://apps.who.int/gho/athena/xmart/data-coded.csv?target=EBOLA_MEASURE/CASES,DEATHS&filter=COUNTRY:GIN;COUNTRY:UNSPECIFIED;COUNTRY:LBR;COUNTRY:UNSPECIFIED;COUNTRY:MLI;COUNTRY:UNSPECIFIED;COUNTRY:GBR;COUNTRY:UNSPECIFIED;COUNTRY:SLE;COUNTRY:UNSPECIFIED;LOCATION:-;INDICATOR_TYPE:SITREP_CUMULATIVE;INDICATOR_TYPE:SITREP_CUMULATIVE_21_DAYS;SEX:-;DATAPACKAGEID:', date)
  
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
    # Cases 21 Days
    df$Indicator <- ifelse(
      df$EBOLA_MEASURE == 'CASES' & 
        df$CASE_DEFINITION == 'CONFIRMED' &
        df$INDICATOR_TYPE == 'SITREP_CUMULATIVE_21_DAYS',
      'Number of confirmed Ebola cases in the last 21 days',
      df$Indicator
    )
    df$Indicator <- ifelse(
      df$EBOLA_MEASURE == 'CASES' & 
        df$CASE_DEFINITION == 'PROBABLE' &
        df$INDICATOR_TYPE == 'SITREP_CUMULATIVE_21_DAYS',
      'Number of probable Ebola cases in the last 21 days',
      df$Indicator
    )
    df$Indicator <- ifelse(
      df$EBOLA_MEASURE == 'CASES' & 
        df$CASE_DEFINITION == 'SUSPECTED' &
        df$INDICATOR_TYPE == 'SITREP_CUMULATIVE_21_DAYS',
      'Number of suspected Ebola cases in the last 21 days',
      df$Indicator
    )
    df$Indicator <- ifelse(
      df$EBOLA_MEASURE == 'CASES' & 
        df$CASE_DEFINITION == 'CONF_PROB_SUSP' &
        df$INDICATOR_TYPE == 'SITREP_CUMULATIVE_21_DAYS',
      'Number of confirmed, probable and suspected Ebola cases in the last 21 days',
      df$Indicator
    )
    
    # Cases
    df$Indicator <- ifelse(
      df$EBOLA_MEASURE == 'CASES' & 
        df$CASE_DEFINITION == 'CONFIRMED' &
        df$INDICATOR_TYPE == 'SITREP_CUMULATIVE',
      'Cumulative number of confirmed Ebola cases',
      df$Indicator
    )
    df$Indicator <- ifelse(
      df$EBOLA_MEASURE == 'CASES' & 
        df$CASE_DEFINITION == 'PROBABLE' &
        df$INDICATOR_TYPE == 'SITREP_CUMULATIVE',
      'Cumulative number of probable Ebola cases',
      df$Indicator
    )
    df$Indicator <- ifelse(
      df$EBOLA_MEASURE == 'CASES' & 
        df$CASE_DEFINITION == 'SUSPECTED' &
        df$INDICATOR_TYPE == 'SITREP_CUMULATIVE',
      'Cumulative number of suspected Ebola cases',
      df$Indicator
    )
    df$Indicator <- ifelse(
      df$EBOLA_MEASURE == 'CASES' & 
        df$CASE_DEFINITION == 'CONF_PROB_SUSP' &
        df$INDICATOR_TYPE == 'SITREP_CUMULATIVE',
      'Cumulative number of confirmed, probable and suspected Ebola cases',
      df$Indicator
    )
    
    # Deaths
    df$Indicator <- ifelse(
      df$EBOLA_MEASURE == 'DEATHS' & 
        df$CASE_DEFINITION == 'CONFIRMED' &
        df$INDICATOR_TYPE == 'SITREP_CUMULATIVE',
      'Cumulative number of confirmed Ebola deaths',
      df$Indicator
    )
    df$Indicator <- ifelse(
      df$EBOLA_MEASURE == 'DEATHS' & 
        df$CASE_DEFINITION == 'PROBABLE' &
        df$INDICATOR_TYPE == 'SITREP_CUMULATIVE',
      'Cumulative number of probable Ebola deaths',
      df$Indicator
    )
    df$Indicator <- ifelse(
      df$EBOLA_MEASURE == 'DEATHS' & 
        df$CASE_DEFINITION == 'SUSPECTED' &
        df$INDICATOR_TYPE == 'SITREP_CUMULATIVE',
      'Cumulative number of suspected Ebola deaths',
      df$Indicator
    )
    df$Indicator <- ifelse(
      df$EBOLA_MEASURE == 'DEATHS' & 
        df$CASE_DEFINITION == 'CONF_PROB_SUSP' &
        df$INDICATOR_TYPE == 'SITREP_CUMULATIVE',
      'Cumulative number of confirmed, probable and suspected Ebola deaths',
      df$Indicator
    )
    
    ## Exceptions
    #  If the case definition is NA, but the measure is 'Deaths' it's a total.
    df$Indicator <- ifelse(
      df$EBOLA_MEASURE == 'DEATHS' & 
        is.na(df$CASE_DEFINITION)  &
        df$INDICATOR_TYPE == 'SITREP_CUMULATIVE',  # exception
      'Cumulative number of confirmed, probable and suspected Ebola deaths',
      df$Indicator
    )
    
    # if the indicator hasn't been identified,
    # use the two columns to build a new one
    df$Indicator <- ifelse(
      is.na(df$Indicator),  # if it's still NA
      paste(df$EBOLA_MEASURE, df$CASE_DEFINITION, df$INDICATOR_TYPE),
      df$Indicator
    )
    
    # Aggregating case data for exceptions.
    aggregateExceptions <- function(d=NULL, exceptions = c('Mali', 'United Kingdom')) {
      sub <- d[d$EBOLA_MEASURE == 'CASES',]
      for (i in 1:length(exceptions)) {
        country_sub <- sub[sub$COUNTRY == exceptions[i],]
        it <- data.frame(Indicator = "Cumulative number of confirmed, probable and suspected Ebola cases",
                         COUNTRY = exceptions[i],
                         DATAPACKAGEID = d$DATAPACKAGEID[1],
                         Numeric = sum(country_sub$Numeric, na.rm = TRUE),
                         EBOLA_MEASURE = NA,
                         CASE_DEFINITION = NA,
                         INDICATOR_TYPE = NA)
        if(i == 1) out <- it
        else out <- rbind(out,it)
      }
      return(out)
    }

    # adding exception data to output
    exceptions_data <- aggregateExceptions(df)
    data <- rbind(df, exceptions_data)

    # cleaning the indicator type columns
    data$EBOLA_MEASURE <- NULL
    data$CASE_DEFINITION <- NULL
    data$INDICATOR_TYPE <- NULL

    # return output
    return(data)
  }
  
  data <- createIndicators()
  
  # reorganizing the indicators
  # sqlite doesn't match columns
  data <- data.frame(Indicator = data$Indicator,
                     Country = data$COUNTRY,
                     Date = data$DATAPACKAGEID,
                     value = data$Numeric)
  
  # Removing those records that still have NAs
  data <- data[!is.na(data$value),]
  
  # check the database for all the values
  # so you can add values based on the latest
  # observation for each country: Sierra Leone, USA, and Spain
  fetchLegacyDataAndInput <- function(db = NULL, table_name = NULL, date = NULL) {
    
    ##############
    ### Config ###
    ##############
    countries = c('Spain', 'United States of America', 'Senegal', 'Nigeria')
    
    # fetching data from db
    db_name <- paste0(db, ".sqlite")
    db <- dbConnect(SQLite(), dbname = db_name)
    all_data <- dbReadTable(db, table_name)
    dbDisconnect(db)
    
    # adding data
    country_data <- all_data[all_data$Country %in% countries,]
    country_data <- country_data[country_data$Date == as.character(max(as.Date(country_data$Date))),]
    if (is.null(custom_date)) date <- as.character(Sys.Date())
    if (is.na(custom_date)) date <- as.character(Sys.Date())  # if args are not provided
    country_data$Date <- date
    
    return(country_data)
  }
  
  # merging that 
  country_data <- fetchLegacyDataAndInput('scraperwiki', 'who_ebola_case_data', custom_date)
  output <- rbind(data, country_data)

  return(output)
}

# series of tests to check data integrity
# these tests only check for values in the new data
# not in the legacy dataset in the db
checkData <- function(df = NULL) {
  
  cat('-------------------------------\n')
  cat('Running tests:\n')
  cat('-------------------------------\n')
  
  ##############
  ### Config ###
  ##############
  n_countries = 9
  
  ######################
  ### Test Variables ###
  ######################
  n_indicators_cases = as.numeric(summary(df$Indicator == 'Cumulative number of confirmed, probable and suspected Ebola cases')[3])
  n_indicators_deaths = as.numeric(summary(df$Indicator == 'Cumulative number of confirmed, probable and suspected Ebola deaths')[3])
  
  if (is.null(df)) stop("No data provided.")  # sanity check
  if (length(unique(df$Country)) != n_countries) cat("Error: Wrong number of countries.\n")  # checking for the right number of countries
  else cat('Success: Correct number of countries.\n')
  if (n_indicators_cases != n_countries) cat("Error: Some total cases indicators are missing.\n")  # checking for total cases indicators
  else cat('Success: Correct number of total cases indicators.\n')
  if (n_indicators_deaths != n_countries) cat("Error: Some total deaths indicators are missing.\n")  # checking for total deaths indicators
  else cat('Success: Correct number of total deaths indicators.\n')


  cat('-------------------------------\n')
}

# Scraper wrapper
runScraper <- function() {
  cat('-----------------------------\n')
  cat('Collecting current data.\n')
  data <- parseData(args[1])  # add custom date here (run once!)
  checkData(data)
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
