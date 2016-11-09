#!/bin/bash

if [ "$TRAVIS_BRANCH" == "master" ] && [ "$TRAVIS_PULL_REQUEST" == "false" ]; then
    if [ "$TRAVIS_JDK_VERSION" == "oraclejdk8" ]; then
        echo -e "Publishing javadocs..."

        mvn -N io.takari:maven:wrapper -Dmaven=3.3.9
        mvn javadoc:javadoc -DskipTests=true
        cp -R target/site/apidocs $HOME/javadoc
        cd $HOME
        git config --global user.email "travis@travis-ci.org"
        git config --global user.name "travis-ci"
        git clone --quiet --branch=gh-pages https://${GH_TOKEN}@github.com/CorfuDB/CorfuDB gh-pages > /dev/null

        cd gh-pages
        git rm -rf ./javadoc
        cp -Rf $HOME/javadoc ./javadoc
        git add -f .
        git commit -m "Updated javadoc from travis build $TRAVIS_BUILD_NUMBER pushed to github pages"
        git push -fq origin gh-pages > /dev/null

        echo -e "Javadoc Built and published to github pages."
    fi
fi