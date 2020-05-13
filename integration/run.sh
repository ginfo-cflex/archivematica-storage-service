#!/usr/bin/env bash

__dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

cd ${__dir}

docker-compose run --rm archivematica-storage-service

docker-compose down
