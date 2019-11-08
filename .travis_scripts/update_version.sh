#!/bin/bash

set -e

# Update corfu version. Format: {major}.{minor}.{date yyyy-mm-dd}.{build_number}
TRAVIS_BUILD_NUMBER="$1"

# Show warning if build number is not set
if [[ -z "${TRAVIS_BUILD_NUMBER}" ]]; then
    echo "Please pass TRAVIS_BUILD_NUMBER"
else
    ./mvnw build-helper:parse-version versions:set \
    -DnewVersion=\${parsedVersion.majorVersion}.\${parsedVersion.minorVersion}.${TRAVIS_BUILD_NUMBER} \
    -DgenerateBackupPoms=false
fi
