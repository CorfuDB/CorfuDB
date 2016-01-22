#!/bin/bash

if [ "$TRAVIS_BRANCH" == "master" ]; then
    if [ "$TRAVIS_JDK_VERSION" == "oraclejdk8" ]; then
        echo -e "Shipping deb package..."

#this is fragile and needs to account for changes in the filename
        cp -R target/corfu_0.1~SNAPSHOT_all.deb  $HOME/$DEBNAME
        cd $HOME
        git config --global user.email "travis@travis-ci.org"
        git config --global user.name "travis-ci"
        git clone --quiet --branch=debian https://${GH_TOKEN}@github.com/CorfuDB/CorfuDB debian > /dev/null

        cd debian
        cp -Rf $HOME/mvn-repo-current/* .
        git add -f .
        git commit -m "Updated maven repository from travis build $TRAVIS_BUILD_NUMBER"
        git push -fq origin mvn-repo > /dev/null
    fi
fi
