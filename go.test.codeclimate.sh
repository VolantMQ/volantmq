#!/usr/bin/env bash

set -e

echo "starting tests"

for pkg in $(go list ./... | grep -v main); do
    go test -coverprofile=$(echo ${pkg} | tr / -).cover ${pkg}
done

echo "mode: set" > c.out
grep -h -v "^mode:" ./*.cover >> c.out
rm -f *.cover

echo "tests finished"
