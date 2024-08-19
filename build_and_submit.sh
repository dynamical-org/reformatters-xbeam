#!/usr/bin/env bash
set -eou pipefail

VERSION=$1
test -n "$VERSION"
REGION=us-central1
IMAGE="$REGION-docker.pkg.dev/subtle-analyzer-427112-j3/beam-images/reformatters-xbeam:v$VERSION"
TEMPLATE_PATH=gs://dataflow-test-dynamical/pipelines/gefs-forecast.json


docker build . -t $IMAGE
docker push $IMAGE

gcloud dataflow flex-template build $TEMPLATE_PATH --image $IMAGE --sdk-language PYTHON
gcloud dataflow flex-template run gefs-forecast-$VERSION --region $REGION --template-file-gcs-location $TEMPLATE_PATH