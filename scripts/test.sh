#!/bin/bash

pushd $(dirname $0)/..

docker compose build

rm -rf ./.pgdata
for pg_version in 16 17; do
    echo; echo "** testing $pg_version"; echo
    docker compose run --rm tester-$pg_version
done

docker compose down
