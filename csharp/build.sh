#!/bin/bash

export FWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export XBUILDOPT=/verbosity:minimal

if [ -z $builduri ];
then
  export builduri=build.sh
fi

export PROJ_NAME=SparkCLR
export PROJ="$FWDIR/$PROJ_NAME.sln"

echo "===== Building $PROJ ====="

function error_exit() {
  if [ -z $STEP ]; 
  then
    export STEP=$CONFIGURATION 
  fi
  echo "===== Build FAILED for $PROJ -- $STEP with error $RC - CANNOT CONTINUE ====="
  exit 1
}

echo "Restore NuGet packages ==================="
export STEP=NuGet-Restore

nuget restore

export RC=$? && [ $RC -ne 0 ] && error_exit

echo "Build Debug =============================="
export STEP=Debug

export CONFIGURATION=$STEP

export STEP=$CONFIGURATION

xbuild /p:Configuration=$CONFIGURATION $XBUILDOPT $PROJ
export RC=$? && [ $RC -ne 0 ] && error_exit
echo "BUILD ok for $CONFIGURATION $PROJ"

echo "Build Release ============================"
export STEP=Release

export CONFIGURATION=$STEP

xbuild /p:Configuration=$CONFIGURATION $XBUILDOPT $PROJ
export RC=$? && [ $RC -ne 0 ] && error_exit
echo "BUILD ok for $CONFIGURATION $PROJ"

#
# The plan is to build SparkCLR nuget package in AppVeyor (Windows). 
# Comment out this step for TravisCI (Linux) for now.
#
# if [ -f "$PROJ_NAME.nuspec" ];
# then
#   echo "===== Build NuGet package for $PROJ ====="
#   export STEP=NuGet-Pack
# 
#   nuget pack "$PROJ_NAME.nuspec"
#   export RC=$? && [ $RC -ne 0 ] && error_exit
#   echo "NuGet package ok for $PROJ"
# fi

echo "===== Build succeeded for $PROJ ====="
