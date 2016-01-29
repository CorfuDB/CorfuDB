#!/bin/bash

if [ "$TRAVIS_BRANCH" == "master" ] && [ "$TRAVIS_PULL_REQUEST" == "false" ]; then
    if [ "$TRAVIS_JDK_VERSION" == "oraclejdk8" ]; then
        echo -e "Publishing maven repository..."

        mvn deploy
        cp -R target/mvn-repo $HOME/mvn-repo-current
        cd $HOME
        git config --global user.email "travis@travis-ci.org"
        git config --global user.name "travis-ci"
        git clone --quiet --branch=mvn-repo https://${GH_TOKEN}@github.com/CorfuDB/CorfuDB mvn-repo > /dev/null

        cd mvn-repo
        cp -Rf $HOME/mvn-repo-current/* .
        git add -f .
        git commit -m "Updated maven repository from travis build $TRAVIS_BUILD_NUMBER"
        git push -fq origin mvn-repo > /dev/null

        echo -e "Maven artifacts built and pushed to github."
    fi
fi