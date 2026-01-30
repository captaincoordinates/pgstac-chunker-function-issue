#!/bin/bash

pushd $(dirname $0)/..

docker compose build

rm -rf ./.pgdata
for pg_version in 16 17; do
    docker compose run \
        --rm \
        pypgstac-$pg_version \
        pypgstac load collections /input/collection.json

    docker compose run \
        --rm \
        pypgstac-$pg_version \
        pypgstac load items /input/item.json

    echo; echo "** testing $pg_version"; echo
    docker compose run \
        --rm \
        --env PGHOST=pgstac-$pg_version \
        tester
done

docker compose down
