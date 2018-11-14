#!/bin/bash

set -e

tag=$1

export GOOGLE_APPLICATION_CREDENTIALS="${HOME}/service-account.json"

cd ${TRAVIS_BUILD_DIR}

project_version=$(sbt -no-colors version | perl -ne 'print "$1\n" if /info.*(\d+\.\d+\.\d+[^\r\n]*)/' | tail -n 1 | tr -d '\n')
if [[ "${tag}" = *"${project_version}" ]]; then
    sbt "project loader" "runMain com.snowplowanalytics.snowplow.storage.bigquery.loader.Main --project=snowplow-assets \
      --templateLocation=gs://sp-hosted-assets/4-storage/snowplow-bigquery-loader/${tag}/SnowplowBigQueryLoaderTemplate-${tag} \
      --stagingLocation=gs://sp-hosted-assets/4-storage/snowplow-bigquery-loader/${tag}/staging \
      --runner=DataflowRunner \
      --tempLocation=gs://sp-hosted-assets/tmp"
    echo "Published Snowplow BigQuery Loader"

    sbt "project forwarder" "runMain com.snowplowanalytics.snowplow.storage.bigquery.forwarder.Main --project=snowplow-assets \
      --templateLocation=gs://sp-hosted-assets/4-storage/snowplow-bigquery-loader/${tag}/SnowplowBigQueryForwarderTemplate-${tag} \
      --stagingLocation=gs://sp-hosted-assets/4-storage/snowplow-bigquery-loader/${tag}/staging \
      --runner=DataflowRunner \
      --tempLocation=gs://sp-hosted-assets/tmp"
    echo "Published Snowplow BigQuery Forwarder"
else
    echo "Tag version '${tag}' doesn't match version in scala project ('${project_version}'). aborting!"
    exit 1
fi
