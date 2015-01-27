# Script for fetching single resources
# from a dataset on CKAN / HDX.
import urllib
import json

# The URL from the resource in question.
resource = "a02903a9-022b-4047-bbb5-45127b591c85"  # resource for the ebola database
url = "https://data.hdx.rwlabs.org/api/action/resource_show?id="
response = urllib.urlopen(url + resource);
data = json.loads(response.read())

# The URL of the actual file (called 'resource' on CKAN)
# may change. This dataset only has one file / resource.
# As long as there is only one file, this will fetch the
# URL from that file.
fileUrl = data["result"]["url"]

# Checking if the file exists.
if len(fileUrl) <= 1:
	print "There was an error"

# Downloading the file locally
else:
	urllib.urlretrieve (fileUrl, "data/topline-ebola-outbreak-figures.csv")
	print "Download successful."