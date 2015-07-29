# Script to check when the World Health Organization
# releases new Ebola data.

import os
import requests
import urllib
import hashlib
import scraperwiki
import shutil
import lxml.html
import pushbullet

###################
## Configuration ##
###################

pushbullet_key = 'XXX'
PATH = 'tool/data/file.txt'

# For sitreps
#XPATH = '//*[@id="content"]/table/tr[1]/td[1]/div/div/ul/li[1]/a/@href'
#WHO = "http://apps.who.int/gho/data/node.ebola-sitrep.quick-downloads?lang=en"

# For data
XPATH = '//*[@id="content"]/table/tr[1]/td[1]/div/div/ul/li[1]/a/@href'
WHO = 'http://apps.who.int/gho/data/node.ebola-sitrep.ebola-summary?lang=en'
payload = {"type": "link", "title": "WHO updated EVD data", "body": "WHO just updated the Ebola data.", "url": WHO, "channel_tag": "hdx-alerts"}

###############
## Functions ##
###############

def grabLink(XPATH):
    connection = urllib.urlopen(WHO)
    doc =  lxml.html.fromstring(connection.read())
    href = doc.xpath(XPATH)

    # for data page
    file_link = 'http://apps.who.int/gho/data/' + href[0]

    # for sitrep page
    # csv_link = 'http://apps.who.int/gho' + href[0].replace("..", "")  # the link needs a little fixing

    return file_link


# function to download page from
# the web and pipe it locally
def downloadFile(local_file):
    # fetching url
    url = grabLink(XPATH)

    # downloading file
    response = requests.get(url, stream=True)
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
        old_hash = scraperwiki.sqlite.get_var('alert')
        scraperwiki.sqlite.save_var('alert', new_hash)
        new_data = old_hash != new_hash

    # returning a boolean
    return new_data

def checkForAlert(local_file):
    # Checking if there is new data
    # pass True to the first_run parameter
    # if this is the first run.
    update_data = checkNewData(local_file)
    if (update_data == False):
        print "\nNo new data from the WHO."
        return

    # proceed if the hash is different, i.e. update
    print "New data from the WHO. Send alert + grab data."
    pushbullet.sendAlert(pushbullet_key, payload)
    os.system('bash tool/run_scraper.sh')  # run the scraper


###############
## Execution ##
###############

# wrapper call for all functions
def runEverything(p):
    downloadFile(p)
    checkForAlert(p)


# ScraperWiki-specific error handler
try:
    runEverything(PATH)
    # if everything ok
    print "Everything seems to be just fine."
    scraperwiki.status('ok')

except Exception as e:
    print e
    scraperwiki.status('error', 'Check for new files failed.')
    os.system("mail -s 'WHO Alert failed: unknown error..' luiscape@gmail.com")
