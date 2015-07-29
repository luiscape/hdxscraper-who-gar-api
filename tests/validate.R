# series of tests to check data integrity
# these tests only check for values in the new data
# not in the legacy dataset in the db
checkData <- function(df = NULL) {

  if (is.null(df)) stop("No data provided.")  # sanity check
  if (is.data.frame(df) == FALSE) stop("No data.frame provided. No data on this date from WHO.")

  cat('-------------------------------\n')
  cat('Running tests:\n')
  cat('-------------------------------\n')

  ##############
  ### Config ###
  ##############
  n_countries = 11

  ######################
  ### Test Variables ###
  ######################
  n_indicators_cases = as.numeric(summary(df$Indicator == 'Cumulative number of confirmed, probable and suspected Ebola cases')[3])
  n_indicators_deaths = as.numeric(summary(df$Indicator == 'Cumulative number of confirmed, probable and suspected Ebola deaths')[3])

  ######################
  ####### Tests ########
  ######################
  # Checking for the right number of countries.
  if (length(unique(df$Country)) != n_countries) cat("Error: Wrong number of countries.\n")  # checking for the right number of countries
  else cat('Success: Correct number of countries.\n')

  # Checking for the right number of cases indicators.
  if (n_indicators_cases != n_countries) cat("Error: Some total cases indicators are missing.\n")  # checking for total cases indicators
  else cat('Success: Correct number of total cases indicators.\n')

  # Checking for the right number of deaths indicators.
  if (n_indicators_deaths != n_countries) cat("Error: Some total deaths indicators are missing.\n")  # checking for total deaths indicators
  else cat('Success: Correct number of total deaths indicators.\n')

  cat('-------------------------------\n')
}