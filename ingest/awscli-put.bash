IMAGE=IMG_2842.JPG
GRID=fip-test-1

aws --profile=hostedgrid-ingest --endpoint-url http://storage.googleapis.com s3 cp $IMAGE s3://hostedgrid-ingest/$GRID/feeds/$IMAGE
