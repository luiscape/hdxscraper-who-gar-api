import os
import csv
import json

import ckanapi

remote = 'http://data.hdx.rwlabs.org'
APIKey = 'XXXXX'

# ckan will be an instance of ckan api wrapper
ckan = None

# edit this part for reach specific resource.
resources = [
    {
        'resource_id': 'f48a3cf9-110e-4892-bedf-d4c1d725a7d1',
        'path': 'ebola-data-db-format.csv',
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

# upload to datasetore function
def deleteDatastore(ckan_resource_id, resource):
    # let's delete any existing data before we upload again
    ckan.action.datastore_delete(resource_id=ckan_resource_id, force=True)

import sys
if __name__ == '__main__':
    if len(sys.argv) <= 2:
        usage = '''python scripts/delete.py {ckan-instance} {api-key}

e.g.

python scripts/upload.py http://datahub.io/ MY-API-KEY
'''
        print(usage)
        sys.exit(1)

    remote = sys.argv[1]
    apikey = sys.argv[2]
    ckan = ckanapi.RemoteCKAN(remote, apikey=apikey)

    resource = resources[0]
    deleteDatastore(resource['resource_id'], resource)
