#!/bin/bash

export FWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export XBUILDOPT=/verbosity:minimal

if [ $builduri = "" ];
then
  export builduri=build.sh
fi

export PROJ_NAME=Examples
export PROJ="$FWDIR/$PROJ_NAME.sln"

echo "===== Building $PROJ ====="

function error_exit() {
  if [ "$STEP" = "" ]; 
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

echo "===== Build succeeded for $PROJ ====="
