#!/bin/bash

# First we remove all the backup files

find . -name "*.*~" -exec rm -f '{}' +

# Then that pesky README.md~ file

rm README.md~

# And then we proceed to commit

git init
touch *
git add *
git commit -m "autocommit [`date`]"
git remote add origin git@github.com:bworwa/derby
git push -u origin master

exit 0

