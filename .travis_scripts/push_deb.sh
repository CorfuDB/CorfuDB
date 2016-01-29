#!/bin/bash

if [ "$TRAVIS_BRANCH" == "Tracing" ] && [ "$TRAVIS_PULL_REQUEST" == "false" ]; then
    if [ "$TRAVIS_JDK_VERSION" == "oraclejdk8" ]; then
        echo -e "Shipping deb package..."
        gpg --import public.key private.key
        echo -e "Listing installed keys:"
        gpg --list-secret-keys
#this is fragile and needs to account for changes in the filename
        DEBNAME="corfu_0.1.${TRAVIS_BUILD_NUMBER}_all.deb"
        cp -R target/corfu_0.1~SNAPSHOT_all.deb  $HOME/$DEBNAME
        cd $HOME
        git config --global user.email "travis@travis-ci.org"
        git config --global user.name "travis-ci"
        git clone --quiet --branch=debian https://${GH_TOKEN}@github.com/CorfuDB/CorfuDB debian > /dev/null

        cd debian
        reprepo -b . includedeb trusty $HOME/$DEBNAME
        #git add -f .
        #git commit -m "Updated Debian repository from travis build $TRAVIS_BUILD_NUMBER"
        #git push -fq origin debian > /dev/null
    fi
fi
