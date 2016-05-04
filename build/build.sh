#!/bin/bash

export FWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export SPARKCLR_HOME="$FWDIR/runtime"
echo "SPARKCLR_HOME=$SPARKCLR_HOME"

if [ -d "$SPARKCLR_HOME" ];
then
  echo "Delete existing $SPARKCLR_HOME ..."
  rm -r -f "$SPARKCLR_HOME"
fi

[ ! -d "$SPARKCLR_HOME" ] && mkdir "$SPARKCLR_HOME"
[ ! -d "$SPARKCLR_HOME/bin" ] && mkdir "$SPARKCLR_HOME/bin"
[ ! -d "$SPARKCLR_HOME/data" ] && mkdir "$SPARKCLR_HOME/data"
[ ! -d "$SPARKCLR_HOME/lib" ] && mkdir "$SPARKCLR_HOME/lib"
[ ! -d "$SPARKCLR_HOME/samples" ] && mkdir "$SPARKCLR_HOME/samples"
[ ! -d "$SPARKCLR_HOME/scripts" ] && mkdir "$SPARKCLR_HOME/scripts"

echo "Assemble Mobius Scala components"
pushd "$FWDIR/../scala"

# clean the target directory first
mvn clean -q
[ $? -ne 0 ] && exit 1

# Note: Shade-plugin helps creates an uber-package to simplify running samples during CI;
# however, it breaks debug mode in IntellJ. So enable shade-plugin
# only in build.cmd to create the uber-package.
# build the package
mvn package -Puber-jar -q

if [ $? -ne 0 ]
then
	echo "Build Mobius Scala components failed, stop building."
	popd
	exit 1
fi
echo "Mobius Scala binaries"
cp target/spark*.jar "$SPARKCLR_HOME/lib/"
popd

# Any .jar files under the lib directory will be copied to the staged runtime lib tree.
if [ -d "$FWDIR/lib" ];
then
  echo "Copy extra jar library binaries"
  for g in `ls $FWDIR/lib/*.jar`
  do
    echo "$g"
    cp "$g" "$SPARKCLR_HOME/lib/"
  done
fi

echo "Assemble Mobius C# components"
pushd "$FWDIR/../csharp"

# clean any possible previous build first
./clean.sh

./build.sh

if [ $? -ne 0 ];
then
	echo "Build Mobius C# components failed, stop building."
	popd
	exit 1
fi
echo "Mobius C# binaries"
cp Worker/Microsoft.Spark.CSharp/bin/Release/* "$SPARKCLR_HOME/bin/"

echo "Mobius C# Samples binaries"
# need to include CSharpWorker.exe.config in samples folder
cp Worker/Microsoft.Spark.CSharp/bin/Release/* "$SPARKCLR_HOME/samples/"
cp Samples/Microsoft.Spark.CSharp/bin/Release/* "$SPARKCLR_HOME/samples/"

echo "Mobius Samples data"
cp Samples/Microsoft.Spark.CSharp/data/* "$SPARKCLR_HOME/data/"
popd

echo "Assemble Mobius examples"
pushd "$FWDIR/../examples"
# clean any possible previous build first
./clean.sh
./build.sh

if [ $? -ne 0 ];
then
	echo "Build Mobius C# Examples failed, stop building."
	popd
	exit 1
fi
popd

echo "Assemble Mobius script components"
pushd "$FWDIR/../scripts"
cp *.sh  "$SPARKCLR_HOME/scripts/"
popd

echo "zip run directory"
[ ! -d "$FWDIR/target" ] && mkdir "$FWDIR/target"
pushd "$SPARKCLR_HOME"
zip -r "$FWDIR/target/run.zip" ./*
popd
