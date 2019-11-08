#!/bin/bash

set -e

echo -e "updating project version..."

git config --global user.email "travis@travis-ci.org"
git config --global user.name "travis-ci"
git add -f .
git commit -m "update project version to: $TRAVIS_BRANCH"
git push -fq origin $TRAVIS_BRANCH > /dev/null

echo -e "Done."