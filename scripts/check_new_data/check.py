# Script to check when the World Health Organization
# releases new Ebola data.

import os
import urllib
import hashlib
import requests

import shutil
import lxml.html
import pushbullet
import scraperwiki

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
PUSBULLET_PAYLOAD = { 
     "type": "link", 
     "title": "WHO updated EVD data", 
     "body": "WHO just updated the Ebola data.", 
     "url": WHO, "channel_tag": "hdx-alerts"
     }


###########
## Logic ##
###########

def grabLink(XPATH):
    '''Fetch specific URL from page based on XPath.'''

    #
    # Downloads HTML page and parses
    # it with provided XPATH.
    #
    connection = urllib.urlopen(WHO)
    doc =  lxml.html.fromstring(connection.read())
    href = doc.xpath(XPATH)

    #
    # Find the download link on
    # on the WHO page.
    #
    file_link = 'http://apps.who.int/gho/data/' + href[0]

    return file_link


def downloadFile(local_file):
    '''Download a specific page from the web based
       on an XPath argument.'''
    
    #
    # Get URL from a WHO page.
    #
    url = grabLink(XPATH)

    #
    # Download page locally.
    #
    response = requests.get(url, stream=True)
    with open(local_file, 'wb') as out_file:
        shutil.copyfileobj(response.raw, out_file)

    del response


def checkNewData(local_file):
    ''' Checks if hash of file has changed. If it has it will
        return True.'''
    
    #
    # Create instance of a SHA1
    # hashing algorithm.
    #
    hasher = hashlib.sha1()
    with open(local_file, 'rb') as afile:
        buf = afile.read()
        hasher.update(buf)
        new_hash = hasher.hexdigest()
        
        #
        # Checking if the files are identical or if
        # they have changed.
        #
        old_hash = scraperwiki.sqlite.get_var('alert')
        scraperwiki.sqlite.save_var('alert', new_hash)
        new_data = old_hash != new_hash

    #
    # Is there new data?
    #
    return new_data

def checkForAlert(local_file):
    '''Checking if there is new data.'''

    update_data = checkNewData(local_file)
    if (update_data == False):
        print "\nNo new data from the WHO."
        return

    # proceed if the hash is different, i.e. update
    print "New data from the WHO. Send alert + grab data."
    pushbullet.sendAlert(pushbullet_key, PUSBULLET_PAYLOAD)
    os.system('bash bin/run_scraper.sh')  # run the scraper


def Main(p):
    '''Wrapper.'''

    downloadFile(p)
    checkForAlert(p)



if __name__ == '__main__':
    
    #
    # Error handler for ScraperWiki messages.
    #
    try:
        Main(PATH)
        print "Everything seems to be just fine."
        scraperwiki.status('ok')

    except Exception as e:
        print e
        scraperwiki.status('error', 'Check for new files failed.')
        os.system("mail -s 'WHO Alert failed: unknown error..' luiscape@gmail.com")
