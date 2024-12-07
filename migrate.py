import requests
from dateutil.parser import parse
import datetime
import shutil
import json

# Copy images from one Grid to another using Grid API.
# Iterates through the source images by upload date.
# Downloads the original image and reuploads it to the destination Grid.
# Preserves the original upload time and metadata.

# Source grid details
source_images_endpoint = "https://TODO.hostedgrid.app/media-api/images"
source_grid_api_key = ""

# Destination grid details
destination_loader_endpoint = "https://TODO2.hostedgrid.app/image-loader/images"
destination_metadata_endpoint = "https://TODO2.hostedgrid.app/metadata-editor/metadata/"
destination_api_key = ""

# Step through the images using upload time as our pagination key until no more records are found
def iterate_images(process_image):
	source_api_auth_headers = {'X-Gu-Media-Key': source_grid_api_key}
	page_size = 10

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
			found[image["data"]["id"]] = 1
			process_image(image)
			next_since = image["data"]["uploadTime"]
			todo -= 1

		# The since filter is non inclusive. If there are more items with the exact uploadTime as the last item on a given we could miss them
		# Roll back the since parameter by 1 milli to correct for this at the risk of entering an infinite loop and some duplicate work
		# The Grid does not seem to support a subordering by id which would help to break these ties.
		corrected = parse(next_since) - datetime.timedelta(milliseconds=1)
		since = corrected.strftime( "%Y-%m-%dT%H:%M:%S.%f%z")
		print(str(todo) + " remaining")

	print("Found: " + str(len(found)) + " unique images")


def migrate_image(image):
	print(image["data"]["id"] + " " + image["data"]["uploadedBy"])
	destination_api_auth_headers = {'X-Gu-Media-Key': destination_api_key}

	# Download original image
	originalPath = "originals/" + image["data"]["id"]
	secureUrl = image["data"]["source"]["secureUrl"]
	response = requests.get(secureUrl, stream=True)
	with open(originalPath, 'wb') as out_file:
  		shutil.copyfileobj(response.raw, out_file)

	# Post original image to image loader
	with open(originalPath, 'rb') as file:
		# Perserving upload information from original
		# TODO Are identifiers needed?
		params = {'uploadTime': image["data"]["uploadTime"], 'uploadedBy': image["data"]["uploadedBy"], 'filename': image["data"]["uploadInfo"]['filename']}
		upload = requests.post(destination_loader_endpoint, data=file, headers=destination_api_auth_headers, params=params)
		if upload.status_code != 200:
			raise Exception('Failed to upload: ' + str(upload.json()))

	# Migrate metadata
	metadata = image['data']['userMetadata']['data']['metadata']
	metadata_put_url = destination_metadata_endpoint + image['data']['id'] + '/metadata'
	metadata_put_headers = {'X-Gu-Media-Key': destination_api_key, 'Content-Type': 'application/json'}
	put_metadata = requests.put(metadata_put_url, data=json.dumps(metadata), headers=metadata_put_headers)
	if put_metadata.status_code != 200:
        	raise Exception('Failed to set metadata: ' + str(put_metadata.json()))

	# Migrate archived
	archived = image['data']['userMetadata']['data']['archived']
	archived_put_url = destination_metadata_endpoint + image['data']['id'] + '/archived'
	put_archived = requests.put(archived_put_url, data=json.dumps(archived), headers=metadata_put_headers)
	if put_archived.status_code != 200:
		raise Exception('Failed to set archived: ' + str(put_archived.text))

	# Migrate labels
	labels = image['data']['userMetadata']['data']['labels']['data']
	label_names = []
	for label in labels:
		label_name = label['data']
		label_names.append(label_name)

	if len(label_names) > 0:
		labels_post_endpoint = destination_metadata_endpoint + image['data']['id'] + '/labels'
		payload = {
			'data': label_names
		}
		post_labels = requests.post(labels_post_endpoint, data=json.dumps(payload), headers=metadata_put_headers)
		if post_labels.status_code != 200:
                	raise Exception('Failed to set labels: ' + str(post_labels.json()))

iterate_images(migrate_image)
