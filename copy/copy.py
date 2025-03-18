import requests
from dateutil.parser import parse
import datetime
import shutil
import json
import os
import numpy
from multiprocessing import Process
import hashlib


# Copy images from one Grid to another using the Grid API.
# Downloads the original image and reuploads it to the destination Grid.
# Preserves the original upload time and metadata.

# Source grid details
source_images_endpoint = "https://demo.hostedgrid.app/media-api/images"
source_grid_api_key = ""

# Destination grid details
destination_loader_endpoint = "https://demo2.hostedgrid.app/image-loader/images"
destination_metadata_endpoint = "https://demo2.hostedgrid.app/metadata-editor/metadata/"
destination_collections_endpoint = "https://demo2.hostedgrid.app/collections/images/"
destination_usage_rights_endpoint="https://demo2.hostedgrid.app/metadata-editor/metadata/"
destination_api_key = ""


def migrate_image(image):
	print("Upload " + image["data"]["id"] + " " + image["data"]["uploadedBy"])
	destination_api_auth_headers = {'X-Gu-Media-Key': destination_api_key}

	# Download original image if not already cached
	originalPath = "originals/" + image["data"]["id"]
	print(originalPath)
	cached = os.path.isfile(originalPath)
	if not cached:
		secureUrl = image["data"]["source"]["secureUrl"]
		print(secureUrl)
		response = requests.get(secureUrl, stream=True)
		with open(originalPath, 'wb') as out_file:
  			shutil.copyfileobj(response.raw, out_file)

	with open(originalPath, 'rb') as file:
		expected_digest = image["data"]["id"]
		sha1 = hashlib.sha1()
		sha1.update(file.read())
		digest = sha1.hexdigest()
		print("Checking digest", expected_digest, digest)
		file.close()
		if not digest == expected_digest:
			raise Exception('Incorrect digest! ' + expected_digest)

	# Validate source image
	identify = os.system("identify -regard-warnings " + originalPath)
	if identify > 0:
		raise Exception('Invalid source! ' + expected_digest)

	print("Uploading")
	# Post original image to image loader
	with open(originalPath, 'rb') as file:
		# Perserving upload information from original
		# TODO Are identifiers needed?
		params = {'uploadTime': image["data"]["uploadTime"], 'uploadedBy': image["data"]["uploadedBy"]}
		if 'filename' in image["data"]["uploadInfo"]:
			params['filename'] = image["data"]["uploadInfo"]["filename"]

		upload = requests.post(destination_loader_endpoint, data=file, headers=destination_api_auth_headers, params=params)
		if upload.status_code != 202:
			raise Exception('Failed to upload: ' + str(upload.json()))

	metadata_put_headers = {'X-Gu-Media-Key': destination_api_key, 'Content-Type': 'application/json'}

	# Overwrite Usage rights
	usage_rights_put_url = destination_usage_rights_endpoint + image['data']['id'] + "/usage-rights"
	payload = {
		'data': {
			"category": "agency",
			"supplier": "Test"
		}
       	}
	put_usage_rights = requests.put(usage_rights_put_url, data=json.dumps(payload), headers=metadata_put_headers)
	if put_usage_rights.status_code != 200:
                raise Exception('Failed to set usage rights: ' + str(put_usage_rights.text))

	# Migrate metadata
	metadata = image['data']['userMetadata']['data']['metadata']
	metadata_put_url = destination_metadata_endpoint + image['data']['id'] + '/metadata'
	put_metadata = requests.put(metadata_put_url, data=json.dumps(metadata), headers=metadata_put_headers)
	if put_metadata.status_code != 200:
        	raise Exception('Failed to set metadata: ' + str(put_metadata.json()))

	# Migrate archived
	archived = {
		"data": True
	}
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

	# Photoshoot
	if 'data' in image["data"]["userMetadata"]['data']['photoshoot']:
		photoshoot_post_endpoint = destination_metadata_endpoint + image['data']['id'] + '/photoshoot'
		photoshoot = image['data']['userMetadata']['data']['photoshoot']
		payload = {
			'data': {
				'title':  photoshoot['data']['title']
				}
		}
		put_photoshoot = requests.put(photoshoot_post_endpoint, data=json.dumps(payload), headers=metadata_put_headers)
		if put_photoshoot.status_code != 200:
			raise Exception('Failed to set photoshoot: ' + str(put_photoshoot.json()))


	# Collections
	collections = image['data']['collections']
	for collection in collections:
		collections_post_endpoint =destination_collections_endpoint + image['data']['id']
		with_root = ['Home'] + collection['data']['path']
		payload = {
                        'data': with_root
                }
		post_collection = requests.post(collections_post_endpoint, data=json.dumps(payload), headers=metadata_put_headers)
		if post_collection.status_code != 200:
			raise Exception('Failed to set collection: ' + str(post_collection.json()))


	open("completed/" + image['data']['id'], 'w')
	print('Done')

def copy_image(image_id):
	if os.path.isfile("completed/" + image_id):
		#print("Already Done")
		return

	image = None
	cachePath = "cache/" + image_id + ".json"
	if not os.path.isfile(cachePath):
		source_api_auth_headers = {'X-Gu-Media-Key': source_grid_api_key}
		source_image_url = source_images_endpoint + "/" + image_id
		print(source_image_url)
		r = requests.get(source_image_url, headers=source_api_auth_headers)
		if r.status_code != 200:
			raise Exception('Failed to fetch image: ' + str(r.json()))

		image = r.json()
		with open(cachePath, 'w') as f:
			j = json.dumps(image, indent = 4)
			f.write(j)
			f.close()

	else:
		with open(cachePath) as json_data:
			image = json.load(json_data)


	try:
		migrate_image(image)
	except Exception as e:
		print(e)
		with open("failures.txt", "a") as myfile:
			myfile.write(image_id + "\n")



file = open('all.txt','r')
image_ids = []
for line in file.readlines():
	image_id = line.strip()
	image_ids.append(image_id)



def batch(image_ids):
	i = 0
	for image_id in image_ids:
		copy_image(image_id)
		i = i + 1

batch(image_ids)
