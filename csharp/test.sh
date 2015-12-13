#!/bin/bash

export FWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ "$NUNITCONSOLE" = "" ];
then
  echo "[test.sh] Error - NUNITCONSOLE environment variable is not export"
  echo "[test.sh] Please set NUNITCONSOLE to /path/to/nunit/console"
  exit 1
fi

cd "$FWDIR"
pwd

export CONFIGURATION=Debug
export TESTS=AdapterTest/bin/$CONFIGURATION/AdapterTest.dll
export TEST_ARGS=  

echo "Test assemblies = $TESTS"
mono "$NUNITCONSOLE" $TEST_ARGS $TESTS

if [ $? -ne 0 ];
then
  echo "==== Test failed ===="
  exit 1
else
  echo "==== Test succeeded ===="
fi
