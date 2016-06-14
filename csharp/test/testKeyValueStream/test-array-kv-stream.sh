#!/bin/bash

cd $(dirname $0)
shellDir=$PWD

cd $(dirname $0)/../../..
codeRoot=$PWD

echo "shellDir = $shellDir , codeRoot = $codeRoot"
#export HADOOP_HOME=$codeRoot/build/tools/winutils
#export SPARK_HOME=$codeRoot/build/tools/spark-1.6.1-bin-hadoop2.6
export SPARKCLR_HOME=$codeRoot/build/runtime

cd $shellDir/bin/Debug # && rm -rf checkDir
LZEXE=./testKeyValueStream.exe
CSMONO=mono #$(locate mono)
if [ $# -lt 1 ]; then
	$CSMONO $LZEXE
	exit
fi

## local mode
# $SPARKCLR_HOME/scripts/sparkclr-submit.sh --verbose --exe $LZEXE $PWD $@

## cluster mode
options="--executor-cores 2 --driver-cores 2 --executor-memory 1g --driver-memory 1g"
$SPARKCLR_HOME/scripts/sparkclr-submit.sh --verbose $options --master yarn-cluster --exe $LZEXE $PWD $@ 

