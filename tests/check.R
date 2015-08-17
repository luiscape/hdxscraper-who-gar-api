#
# This script can be used to
# quickly check what are the cummulative
# numbers for deaths and cases.
#

library(dplyr)

db <- dbConnect(SQLite(), dbname = 'scraperwiki.sqlite')
data <- dbReadTable(db, 'ebola_data_db_format')
dbDisconnect(db)


interest = c("Cumulative number of confirmed, probable and suspected Ebola cases", 
             "Cumulative number of confirmed, probable and suspected Ebola deaths")

for (i in 1:length(interest)) {
  indicator = interest[i]
  x <- filter(data, Indicator == indicator, Date == '2015-07-29')
  r = sum(x$value)
  print (paste(indicator, " --> ", r))
}