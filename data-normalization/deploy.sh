#!/usr/bin/env bash

set -eE

./setup.sh
./run.sh
rm -rf data-repo
git clone https://github.com/matthewscholefield/streetlights -b data --single-branch data-repo
cd data-repo
cp ../data/combined.json .
git add combined.json
git commit --amend --no-edit
git push origin data --force
cd ..
rm -rf data-repo
