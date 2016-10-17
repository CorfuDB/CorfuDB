#!/usr/bin/env bash

echo -e "Setting up project environment"

mvn -N io.takari:maven:wrapper -Dmaven=3.3.9

# replace SNAPSHOT with actual version
export PROJECT_VERSION="$(mvn exec:exec -Dexec.executable="echo" \
                -Dexec.args='${project.version}' --non-recursive -q \
                | tr -d '\n' | sed 's/-SNAPSHOT/-'$TRAVIS_BUILD_NUMBER'/')"

mvn versions:set -DnewVersion=$PROJECT_VERSION

echo -e "Project version set to ${PROJECT_VERSION}"

echo $PROJECT_VERSION > $HOME/.project_version