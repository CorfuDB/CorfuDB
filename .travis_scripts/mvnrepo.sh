#!/bin/bash

if [ "$TRAVIS_BRANCH" == "master" ] && [ "$TRAVIS_PULL_REQUEST" == "false" ]; then
    if [ "$TRAVIS_JDK_VERSION" == "oraclejdk8" ]; then
        echo -e "Publishing maven repository..."

        PROJECT_VERSION=$(cat ${HOME}/.project_version)
        mvn -N io.takari:maven:wrapper -Dmaven=3.3.9
        mvn deploy -DskipTests=true
        #delete debian artifacts before deploy
        rm target/mvn-repo/org/corfudb/corfu/${PROJECT_VERSION}/*.deb
        rm target/mvn-repo/org/corfudb/corfu/${PROJECT_VERSION}/*.deb.md5
        rm target/mvn-repo/org/corfudb/corfu/${PROJECT_VERSION}/*.deb.sha1
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