#!/bin/bash

tag=$1

file="${HOME}/.dockercfg"
docker_repo="snowplow-docker-registry.bintray.io"
curl -X GET \
    -u${BINTRAY_SNOWPLOW_DOCKER_USER}:${BINTRAY_SNOWPLOW_DOCKER_API_KEY} \
    https://${docker_repo}/v2/auth > $file

cd $TRAVIS_BUILD_DIR

project_version=$(sbt "project common" version -Dsbt.log.noformat=true | perl -ne 'print "$1\n" if /info.*(\d+\.\d+\.\d+[^\r\n]*)/' | tail -n 1 | tr -d '\n')
if [[ "${tag}" = *"${project_version}" ]]; then
    sbt "project loader" docker:publishLocal
    sbt "project mutator" docker:publishLocal
    sbt "project repeater" docker:publishLocal
    sbt "project forwarder" docker:publishLocal
    docker push "${docker_repo}/snowplow/snowplow-bigquery-mutator:${tag}"
    docker push "${docker_repo}/snowplow/snowplow-bigquery-loader:${tag}"
    docker push "${docker_repo}/snowplow/snowplow-bigquery-repeater:${tag}"
    docker push "${docker_repo}/snowplow/snowplow-bigquery-forwarder:${tag}"
else
    echo "Tag version '${tag}' doesn't match version in scala project ('${project_version}'). Aborting!"
    exit 1
fi

