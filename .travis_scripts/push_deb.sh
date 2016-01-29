#!/bin/bash

if [ "$TRAVIS_BRANCH" == "master" ] && [ "$TRAVIS_PULL_REQUEST" == "false" ]; then
    if [ "$TRAVIS_JDK_VERSION" == "oraclejdk8" ]; then
        echo -e "Shipping deb package..."
        mvn jdeb:jdeb
        gpg --import public.key private.key
#this is fragile and needs to account for changes in the filename
        DEBNAME="corfu_0.1.${TRAVIS_BUILD_NUMBER}_all.deb"
        echo -e "Debian package to be output: ${DEBNAME}"
        cp -R target/corfu_0.1~SNAPSHOT_all.deb  $HOME/$DEBNAME
        cd $HOME
        git config --global user.email "travis@travis-ci.org"
        git config --global user.name "travis-ci"
        git clone --quiet --branch=debian https://${GH_TOKEN}@github.com/CorfuDB/CorfuDB debian > /dev/null

        cd debian
        reprepro -b . includedeb trusty $HOME/$DEBNAME
        git add -f .
        git commit -m "Updated Debian repository from travis build $TRAVIS_BUILD_NUMBER"
        git push -fq origin debian > /dev/null
    fi
fi
