# Unfinished script that downloads
# a file from the web, stores it locally,
# and uploads that file as a resource update
# on HDX. This is done to avoid external linking
# on HDX.

### UNFINISHED ###

import requests
import shutil

api_key = 'a6863277-f35e-4f50-af85-78a2d9ebcdd3'
PATH = 'data/ebola-data-db-format.xls'
format = "XLS"
resource_id = 'a8b51b81-1fa7-499d-a9f2-3d0bce06b5b5'

def fetchFileFromResource(resource_id, p):
	print("Downloading file...")
	resource_show_url = 'https://data.hdx.rwlabs.org/api/action/resource_show?id=' + resource_id
	doc = requests.get(resource_show_url)
	file_url = doc.json()["result"]["url"]
	response = requests.get(file_url, stream=True)
	with open(p, 'wb') as out_file:
	    shutil.copyfileobj(response.raw, out_file)
	del response
	print("Done.")

def updateResource(resource_id, p):
	print("Uploading file...")
	update_url = 'https://data.hdx.rwlabs.org/api/action/resource_update'
	requests.post(
		update_url,
		data={"id":resource_id, "format": format},
		headers={"Authorization": api_key},
		files=[('upload', file(p))])

	print("Done.")


fetchFileFromResource(resource_id, PATH)
updateResource(resource_id, PATH)