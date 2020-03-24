#!/bin/bash

set -e

if [ -z "$WORKSPACE" ]; then
    WORKSPACE=$(pwd)
fi
if [ -z "$1" ]; then
    echo "Invalid parameters"
    echo "Usage: ./corfu_artifacts_uploader.sh VERSION_NUMBER"
    exit 1
fi
corfu_version_number=$1

download_corfu_build () {
    echo "==== downloading corfu build into workspace ===="
    rm -rf $WORKSPACE/publish
    mkdir -p $WORKSPACE/publish/mvn/org/corfudb
    cd $WORKSPACE/publish
    srcdir=~/.m2/repository/org/corfudb
    cp -r $srcdir/* $WORKSPACE/publish/mvn/org/corfudb
}

upload_to_artifactory () {
    echo "==== uploading corfu build into artifactory ===="
    cd $WORKSPACE/publish
    artifactory_api_key="AKCp5aTbQzwUVXU4VZRhuggzb2QcDb8PuA1aRhHTAdNySH4KqBbPahcsjmG9Yb2in3He1r94K"
    artifactory_repo=nsx-transformers-maven-local
    artifactory_root_url="https://build-artifactory.eng.vmware.com/artifactory"

    # 2. Upload corfu debian packages into nsx-corfu directory
    # Note the debian package shouldn't be used when it is in maven directory

    cp ../debian/target/*_all.deb ./

    debian_local_paths=$(find . -name "*.deb")
    for debian_local_path in $debian_local_paths; do
        if [ $(echo $debian_local_path | cut -d/ -f 2) = "mvn" ]; then
            continue
        fi
        debian_name=$(echo $debian_local_path | rev | cut -d/ -f 1 | rev)
        debian_remote_path=org/corfudb/nsx-corfu/$corfu_version_number/$debian_name
        curl -H "X-JFrog-Art-Api: $artifactory_api_key" \
            -T $debian_local_path \
            -X PUT "$artifactory_root_url/$artifactory_repo/$debian_remote_path"
    done

    # 3. Upload pom files
    pom_local_paths=$(find . -name "*.pom")
    for pom_local_path in $pom_local_paths; do
        pom_remote_path=$(echo $pom_local_path | cut -d/ -f 3-)
        curl -H "X-JFrog-Art-Api: $artifactory_api_key" \
            -T $pom_local_path \
            -X PUT "$artifactory_root_url/$artifactory_repo/$pom_remote_path"
    done

    # 4. Upload jar files
    jar_local_paths=$(find . -name "*.jar")
    for jar_local_path in $jar_local_paths; do
        if [ $(echo $jar_local_path | cut -d/ -f 2) != "mvn" ]; then
            continue
        fi
        jar_remote_path=$(echo $jar_local_path | cut -d/ -f 3-)
        curl -H "X-JFrog-Art-Api: $artifactory_api_key" \
            -T $jar_local_path \
            -X PUT "$artifactory_root_url/$artifactory_repo/$jar_remote_path"
    done

    # 5. Upload md5 checksum files
    md5_local_paths=$(find . -name "*.md5")
    for md5_local_path in $md5_local_paths; do
        if [ $(echo $md5_local_path | rev | cut -d. -f 2 | rev) = "xml" ]; then
            continue
        fi
        md5_remote_path=$(echo $md5_local_path | cut -d/ -f 3-)
        curl -H "X-JFrog-Art-Api: $artifactory_api_key" \
            -T $md5_local_path \
            -X PUT "$artifactory_root_url/$artifactory_repo/$md5_remote_path"
    done

    # 6. Upload sha1 checksum files
    sha1_local_paths=$(find . -name "*.sha1")
    for sha1_local_path in $sha1_local_paths; do
        if [ $(echo $sha1_local_path | rev | cut -d. -f 2 | rev) = "xml" ]; then
            continue
        fi
        sha1_remote_path=$(echo $sha1_local_path | cut -d/ -f 3-)
        curl -H "X-JFrog-Art-Api: $artifactory_api_key" \
            -T $sha1_local_path \
            -X PUT "$artifactory_root_url/$artifactory_repo/$sha1_remote_path"
    done
}

main () {
    download_corfu_build
    upload_to_artifactory
}

main
