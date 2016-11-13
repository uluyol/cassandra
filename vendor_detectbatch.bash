#!/usr/bin/env bash

set -e

if [[ ! -d $1 ]]; then
	echo "usage: vendor_detectbatch.bash /path/to/detect/batch/source" >&2
	exit 2
fi

(cd "$1" && make)
cp "$1"/detectbatch.* "${0%/*}/src/resources/org/apache/cassandra/ratelimit/"

