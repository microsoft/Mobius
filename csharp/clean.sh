#!/bin/bash

#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for full license information.
#

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
