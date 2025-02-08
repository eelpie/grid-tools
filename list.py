import requests
from dateutil.parser import parse
import json
import datetime
import sys

# Source grid details
source_images_endpoint = "https://TODO.hostedgrid.app/media-api/images"
source_grid_api_key = ""

# Step through the images using upload time as our pagination key until no more records are found
def iterate_images(process_image):
	source_api_auth_headers = {'X-Gu-Media-Key': source_grid_api_key}
	page_size = 100

	since = "1970-01-01T12:00:00.000Z"
	found = {}

	todo = -1
	last_todo = 0
	while todo != 0:
		next_since = since
		params = {'orderBy': 'uploadTime', 'length': page_size, 'since': since}
		r = requests.get(source_images_endpoint, headers=source_api_auth_headers, params=params)
		if r.status_code != 200:
			raise Exception('Failed to fetch images: ' + str(r.json()))

		images = r.json()["data"]
		todo = r.json()['total']

		# Check that we are still making forward progress
		if todo == last_todo:
			raise Exception('Pagination is stuck in a block of items with the same uploadTime. Increasing page_size may help')
		last_todo = todo

        	# Foreach image on page
		for image in images:
			id = image["data"]["id"]
			if found.get(id) is None:
				found[id] = 1
				process_image(image)
			next_since = image["data"]["uploadTime"]
			todo -= 1

		# The since filter is non inclusive. If there are more items with the exact uploadTime as the last item on a given we could miss them
		# Roll back the since parameter by 1 milli to correct for this at the risk of entering an infinite loop and some duplicate work
		# The Grid does not seem to support a subordering by id which would help to break these ties.
		corrected = parse(next_since) - datetime.timedelta(milliseconds=1)
		since = corrected.strftime( "%Y-%m-%dT%H:%M:%S.%f%z")
		sys.stderr.write(str(todo) + " remaining\n")
		sys.stderr.flush()

	sys.stderr.write("Found: " + str(len(found)) + " unique images\n")
	sys.stderr.flush()

def list_image(image):
	print(image["data"]["id"])

iterate_images(list_image)
