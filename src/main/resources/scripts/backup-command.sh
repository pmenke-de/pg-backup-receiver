#!/bin/bash
# $1 path to WAL segment
# $2 name of WAL segment
# $3 name of datbase node
# $4 HTTP credentials
curl -u$4 --write-out %{http_code} --silent --output /dev/null "https://menke.network/pg_backup/wal_segment/$3/$2" --upload-file "$1" | grep 201 > /dev/null && exit 0 || exit 1