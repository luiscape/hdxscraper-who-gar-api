## Script to push notifications using
## Pushbullet.

# For more information about Pushbullet and its
# API, please visit the service's web-page:
# https://docs.pushbullet.com/v2/pushes/

import json
import requests as r

# main alerting function
def sendAlert(key, payload):

	# defining authorization
	key_string = "Bearer " + key
	headers = {'content-type': 'application/json', "Authorization": key_string }

	# sending notification
	r.post('https://api.pushbullet.com/v2/pushes',
			data=json.dumps(payload),
			headers=headers)