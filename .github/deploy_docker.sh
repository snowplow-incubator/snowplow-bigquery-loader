#!/bin/bash

set -e

tag=$1

mkdir -p "$HOME/.docker"
file="$HOME/.docker/config.json"
docker_repo="snowplow-docker-registry.bintray.io"
cfg=$(curl -X GET -u${BINTRAY_SNOWPLOW_DOCKER_USER}:${BINTRAY_SNOWPLOW_DOCKER_API_KEY} "https://${docker_repo}/v2/auth")
echo '{"auths": '"$cfg"'}' > $file


project_version=$(sbt "project common" version -Dsbt.log.noformat=true | perl -ne 'print "$1\n" if /info.*(\d+\.\d+\.\d+[^\r\n]*)/' | tail -n 1 | tr -d '\n')
if [[ "${tag}" = *"${project_version}" ]]; then
    echo "Building and publishing ${docker_repo}/snowplow/snowplow-bigquery-repeater:${tag} ($project_version version)"
    sbt "project repeater" docker:publishLocal
    docker push "${docker_repo}/snowplow/snowplow-bigquery-repeater:${tag}"
else
    echo "Tag version '${tag}' doesn't match version in scala project ('${project_version}'). Aborting!"
    exit 1
fi
