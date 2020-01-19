#!/usr/bin/env bash

SEMVER_REGEX="^v|V?(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)(\\-[0-9A-Za-z-]+(\\.[0-9A-Za-z-]+)*)?(\\+[0-9A-Za-z-]+(\\.[0-9A-Za-z-]+)*)?$"

tag=$(git describe --abbrev=0 --tags)
commits=$(git rev-list ${tag}.. --count)
version=${tag}

if [[ ${commits} -gt 0 ]]; then
    version=${version}-rc.${commits}
fi

echo ${version}
