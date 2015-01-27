# Simple script that manages the creation of
# datastores in CKAN / HDX.

import os
import csv
import json
import scraperwiki
import ckanapi
import urllib
import requests
import sys
import hashlib

# Collecting configuration variables
PATH = 'tool/data/ebola-data-db-format.csv'
remote = 'https://data.hdx.rwlabs.org'
resource_id = sys.argv[1]
apikey = sys.argv[2]

# ckan will be an instance of ckan api wrapper
ckan = None

# Function to download a resource from CKAN.
def downloadResource(filename):

    # querying
    url = 'https://data.hdx.rwlabs.org/api/action/resource_show?id=' + resource_id
    r = requests.get(url)
    doc = r.json()
    fileUrl = doc["result"]["url"]

    # downloading
    try:
        urllib.urlretrieve(fileUrl, filename)
    except:
        print 'There was an error downlaoding the file.'

# Function that checks for old SHA hash
# and stores as a SW variable the new hash
# if they differ. If this function returns true,
# then the datastore is created.
def checkHash(filename, first_run):
    hasher = hashlib.sha1()
    with open(filename, 'rb') as afile:
        buf = afile.read()
        hasher.update(buf)
        new_hash = hasher.hexdigest()

    # checking if the files are identical or if
    # they have changed
    if first_run:
        scraperwiki.sqlite.save_var('datastore', new_hash)
        new_data = False

    else:
        old_hash = scraperwiki.sqlite.get_var('datastore')
        scraperwiki.sqlite.save_var('datastore', new_hash)
        new_data = old_hash != new_hash

    # returning a boolean
    return new_data

def updateDatastore(filename):

    # Checking if there is new data
    new_data = checkHash(filename, first_run = False)
    if (new_data == False):
        print "DataStore Status: No new data. Not updating datastore."
        return

    else:
        print "DataStore Status: New data. Updating datastore."

        # defining the schema
        resources = [
            {
                'resource_id': resource_id,
                'path': filename,
                'schema': {
                    "fields": [
                      { "id": "Indicator", "type": "text" },
                      { "id": "Country", "type": "text" },
                      { "id": "Date", "type": "timestamp"},
                      { "id": "value", "type": "float" }
                    ]
                },
            }
        ]


        def upload_data_to_datastore(ckan_resource_id, resource):
            # let's delete any existing data before we upload again
            try:
                ckan.action.datastore_delete(resource_id=ckan_resource_id, force=True)
            except:
                pass

            ckan.action.datastore_create(
                    resource_id=ckan_resource_id,
                    force=True,
                    fields=resource['schema']['fields'],
                    primary_key=resource['schema'].get('primary_key'))

            reader = csv.DictReader(open(resource['path']))
            rows = [ row for row in reader ]
            chunksize = 10000
            offset = 0
            print('Uploading data for file: %s' % resource['path'])
            while offset < len(rows):
                rowset = rows[offset:offset+chunksize]
                ckan.action.datastore_upsert(
                        resource_id=ckan_resource_id,
                        force=True,
                        method='insert',
                        records=rowset)
                offset += chunksize
                print('Done: %s' % offset)


        if __name__ == '__main__':
            if len(sys.argv) <= 2:
                usage = '''python scripts/upload.py {resource-id} {api-key}

                e.g.

                python scripts/upload.py RESOURCE_ID API_KEY
                '''
                print(usage)
                sys.exit(1)

            ckan = ckanapi.RemoteCKAN(remote, apikey=apikey)

            resource = resources[0]
            upload_data_to_datastore(resource['resource_id'], resource)

def runEverything():
    downloadResource(PATH)
    updateDatastore(PATH)


# Error handler for running the entire script
try:
    runEverything()
    # if everything ok
    print "SW Status: Everything seems to be just fine."
    scraperwiki.status('ok')

except Exception as e:
    print e
    scraperwiki.status('error', 'Creating datastore failed')
    os.system("mail -s 'Ebola Case data: creating datastore failed.' luiscape@gmail.com")
