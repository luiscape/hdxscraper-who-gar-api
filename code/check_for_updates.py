# Script to check when the World Health Organization
# releases new Ebola data.

import os
import requests
import urllib
import hashlib
import scraperwiki
import shutil
import lxml.html
import time

###################
## Configuration ##
###################

PATH = 'tool/data/hash_temp.csv'


###############
## Functions ##
###############

# function to download page from
# the web and pipe it locally
def downloadFile(local_file, date=False):
    # for custom dates
    if (date==False):
        date = time.strftime("%Y-%m-%d")

    # if no custom date is provided, use today's date
    date_url = 'http://apps.who.int/gho/athena/xmart/data-coded.csv?target=EBOLA_MEASURE/CASES,DEATHS&filter=LOCATION:-;DATAPACKAGEID:' + date + ';INDICATOR_TYPE:SITREP_CUMULATIVE;INDICATOR_TYPE:SITREP_CUMULATIVE_21_DAYS;SEX:-'


    # downloading file
    response = requests.get(date_url, stream=True)
    with open(local_file, 'wb') as out_file:
        shutil.copyfileobj(response.raw, out_file)
    del response

# Function that checks for old SHA hash
# and stores as a SW variable the new hash
# if they differ. If this function returns true,
# then the datastore is created.
def checkNewData(local_file):

    hasher = hashlib.sha1()
    with open(local_file, 'rb') as afile:
        buf = afile.read()
        hasher.update(buf)
        new_hash = hasher.hexdigest()

        # checking if the files are identical or if
        # they have changed
        old_hash = scraperwiki.sqlite.get_var('hash')
        scraperwiki.sqlite.save_var('hash', new_hash)
        new_data = old_hash != new_hash

    # returning a boolean
    return new_data


# Action to be taken.
def checkForAlert(local_file):
    # Checking if there is new data
    # pass True to the first_run parameter
    # if this is the first run.
    update_data = checkNewData(local_file)
    if (update_data == False):
        print "\nNo new data from the WHO."
        return

    # proceed if the hash is different, i.e. update
    print "New data from the WHO. Running scraper."
    system('bash tool/run_scraper.sh')


###############
## Execution ##
###############

# wrapper call for all functions
def runEverything(p):
    downloadFile(p,date="2015-01-08")
    checkForAlert(p)


# ScraperWiki-specific error handler
try:
    runEverything(PATH)
    # if everything ok
    print "ScraperWiki status: SUCCESS"
    scraperwiki.status('ok')

except Exception as e:
    print "ScraperWiki status: ERROR"
    print e
    scraperwiki.status('error', 'Check for new files failed.')
    os.system("mail -s 'WHO File Check just failed: unknown error..' luiscape@gmail.com")
