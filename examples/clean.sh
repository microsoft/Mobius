#!/bin/bash

for g in `find . -type d -name bin`
do
  rm -r -f "$g"
done

for g in `find . -type d -name obj`
do
  rm -r -f "$g"
done

# for g in `find . -type d -name TestResults`
# do
#   rm -r -f "$g"
# done
