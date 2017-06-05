#!/bin/bash

if [ "$TRAVIS_BRANCH" == "master" ] && [ "$TRAVIS_PULL_REQUEST" == "false" ]; then
    if [ "$TRAVIS_JDK_VERSION" == "oraclejdk8" ]; then
        echo -e "Shipping deb package..."
        mvn -N io.takari:maven:wrapper -Dmaven=3.3.9
        ./mvnw deploy -DskipTests=true
        gpg --import public.key private.key
#this is fragile and needs to account for changes in the filename
        DEBNAME="debian_0.1+${TRAVIS_BUILD_NUMBER}_all.deb"
        CORFUNAME="corfu_0.1+${TRAVIS_BUILD_NUMBER}_all.deb"
        echo -e "Debian package to be output: ${CORFUNAME}"
        cp -R debian/target/$DEBNAME  $HOME/$CORFUNAME
        cd $HOME
        git config --global user.email "travis@travis-ci.org"
        git config --global user.name "travis-ci"
        git clone --quiet --branch=debian https://${GH_TOKEN}@github.com/CorfuDB/Corfu-Repos debian > /dev/null

        cd debian
        reprepro -b . includedeb trusty $HOME/$CORFUNAME
        git add -f .
        git commit -m "Updated Debian repository from travis build $TRAVIS_BUILD_NUMBER"
        git push -fq origin debian > /dev/null
    fi
fi
