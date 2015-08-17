import os
import csv
import sys
import json
import urllib
import ckanapi
import hashlib
import requests
import scraperwiki

# Collecting configuration variables
PATH = 'tool/data/ebola-data-db-format.csv'
REMOTE_CKAN = 'https://data.hdx.rwlabs.org'
resource_id = sys.argv[1]
apikey = sys.argv[2]

# ckan will be an instance of ckan api wrapper
ckan = None


def downloadResource(filename, resource_id):
  '''Downloads a resource from CKAN.'''

  print "Downloading file from CKAN."

  #
  # Querying HDX for download URL.
  #
  url = REMOTE_CKAN + '/api/action/resource_show?id=' + resource_id
  r = requests.get(url)
  doc = r.json()
  fileUrl = doc["result"]["perma_link"]

  #
  # Downloads file.
  #
  try:
    with open(filename, 'wb') as f:
      for chunk in r:
        f.write(chunk)

  except Exception as e:
    print e
    print 'There was an error downlaoding the file.'
    return False


#
# Function that checks for old SHA hash
# and stores as a SW variable the new hash
# if they differ. If this function returns true,
# then the datastore is created.
#
def checkHash(filename, first_run):
  ''' Checks hash of file.'''

  hasher = hashlib.sha1()
  with open(filename, 'rb') as afile:
    buf = afile.read()
    hasher.update(buf)
    new_hash = hasher.hexdigest()

  #
  # Checking if the files are identical or if
  # they have changed.
  #
  if first_run:
    scraperwiki.sqlite.save_var('datastore', new_hash)
    new_data = False

  else:
    old_hash = scraperwiki.sqlite.get_var('datastore')
    scraperwiki.sqlite.save_var('datastore', new_hash)
    new_data = old_hash != new_hash

  #
  # Returning a Boolean.
  #
  return new_data


def updateDatastore(filename):
  '''Updates a CKAN DataStore based on CSV input.'''

  print "Updating DataStore ..."

  #
  # Checking the hashes of files to
  # see if an update is necessary.
  #
  new_data = checkHash(filename, first_run = False)
  if (new_data == False):
    print "DataStore Status: No new data. Not updating datastore."
    return False

  else:
    print "DataStore Status: New data. Updating datastore."

    #
    # Resource schema.
    #
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
        '''Uploads data to a CKAN DataStore.'''

        #
        # Deletes DataStore -- if it exists.
        #
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


def Main():
  '''Wrapper.'''

  ckan = ckanapi.RemoteCKAN(REMOTE_CKAN, apikey=apikey)
  resource = resources[0]
  upload_data_to_datastore(resource['resource_id'], resource)
  downloadResource(PATH)
  updateDatastore(PATH)




if __name__ == '__main__':
  
  #
  # ScraperWiki error handler.
  #
  try:
    runEverything()
    print "SW Status: Everything seems to be just fine."
    scraperwiki.status('ok')

  except Exception as e:
    print e
    scraperwiki.status('error', 'Creating datastore failed')
    os.system("mail -s 'Ebola Case data: creating datastore failed.' luiscape@gmail.com")
