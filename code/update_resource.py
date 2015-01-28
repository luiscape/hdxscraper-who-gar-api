# Script to update resources on HDX.
# This script sends a file attached.

import ckanapi

def uploadResource():
	hdx = ckanapi.RemoteCKAN('https://data.hdx.rwlabs.org',
	    apikey='XXX',
	    user_agent='CKAN_API/1.0')
	try:
		hdx.action.resource_update(
		    id='af4dc3ae-17e1-4a5c-8ddf-8945a90b0a33',
		    upload=open('data/ebola-data-db-format.csv'),
		    format='CSV',
		    description='Ebola data in record format with indicator, country, date and value.'
		    )

	except ckanapi.errors.ValidationError:
	        print 'You have missing parameters. Check the url and type are included.\n'

	except ckanapi.errors.NotFound:
	        print 'Resource not found!\n'
