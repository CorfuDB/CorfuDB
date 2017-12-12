#!/usr/bin/env bash

if [ "$TRAVIS_PULL_REQUEST" != "false" ] && [ "$TRAVIS_SECURE_ENV_VARS" == "true" ]; then
    ./mvnw verify -Dmaven.javadoc.skip=true -Dtest.travisBuild=true  sonar:sonar \
        -Dsonar.analysis.mode=preview \
        -Dsonar.host.url=https://sonarqube.com \
        -Dsonar.github.pullRequest=$TRAVIS_PULL_REQUEST \
        -Dsonar.github.repository=$TRAVIS_REPO_SLUG \
        -Dsonar.github.oauth=$SONAR_GITHUB_TOKEN \
        -Dsonar.organization=corfudb \
        -Dsonar.login=$SONAR_TOKEN
else
    if [ "$TRAVIS_SECURE_ENV_VARS" == "false" ] ; then
        echo "Not submitting sonar status to github because this is an external build"
        ./mvnw verify -Dmaven.javadoc.skip=true -Dsonar.host.url=https://sonarqube.com \
        -Dsonar.organization=corfudb -Dsonar.analysis.mode=preview \
        -Dtest.travisBuild=true sonar:sonar
    else
        ./mvnw verify -Dmaven.javadoc.skip=true -Dsonar.host.url=https://sonarqube.com \
        -Dsonar.organization=corfudb -Dsonar.login=$SONAR_TOKEN -Dtest.travisBuild=true sonar:sonar
    fi
fi

