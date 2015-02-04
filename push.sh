#!/bin/bash
rm -fr *.pyc
git add . --ignore-removal
git commit -m "fix bugs"
git push -u origin master
