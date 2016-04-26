#!/bin/bash

export verbose=

for param in "$@"
do
  case "$param" in
    --verbose) export verbose="--verbose"
    ;;
  esac
done

# setup Hadoop and Spark versions
export SPARK_VERSION=1.6.1
export HADOOP_VERSION=2.6
echo "[run-samples.sh] SPARK_VERSION=$SPARK_VERSION, HADOOP_VERSION=$HADOOP_VERSION"

export FWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# download runtime dependencies: spark
export TOOLS_DIR="$FWDIR/../tools"
[ ! -d "$TOOLS_DIR" ] && mkdir "$TOOLS_DIR"

export SPARK=spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION
export SPARK_HOME="$TOOLS_DIR/$SPARK"
if [ ! -d "$SPARK_HOME" ];
then
  wget "http://www.us.apache.org/dist/spark/spark-$SPARK_VERSION/$SPARK.tgz" -O "$TOOLS_DIR/$SPARK.tgz"
  tar xfz "$TOOLS_DIR/$SPARK.tgz" -C "$TOOLS_DIR"
fi
export PATH="$SPARK_HOME/bin:$PATH"

# update spark verbose mode
if [ ! "$verbose" = "--verbose" ];
then
  # redirect the logs from console (default) to /tmp
  cp "$FWDIR"/spark.conf/*.properties "$SPARK_HOME/conf/"
  sed -i "s/\${env:TEMP}/\/tmp/g" "$SPARK_HOME/conf/log4j.properties"
else
  # remove customized log4j.properties, revert back to out-of-the-box Spark logging to console
  rm -f "$SPARK_HOME"/conf/*.properties
fi

# update sparkclr verbose mode
export SPARKCLRCONF="$FWDIR/../runtime/samples"
export SUFFIX=".original"
if [ ! "$verbose" = "--verbose" ];
then
  for file in `ls "$SPARKCLRCONF"/*.config`
  do
    # backup
    if [ -f "$file$SUFFIX" ];
    then
      cp "$file$SUFFIX" "$file"
    else
      cp "$file" "$file$SUFFIX"
    fi
    sed -i 's/<appender-ref\s*ref="ConsoleAppender"\s*\/>/<\!-- <appender-ref ref="ConsoleAppender" \/> -->/g' "$file"
  done
else
  # restore from original configs
  for file in `ls "$SPARKCLRCONF"/*.config`
  do
    [ -f "$file$SUFFIX" ] && cp "$file$SUFFIX" "$file"
  done
fi


export SPARKCLR_HOME="$FWDIR/../runtime"
export SPARKCSV_JARS=

# run-samples.sh is in local mode, should not load Hadoop or Yarn cluster config. Disable Hadoop/Yarn conf dir.
export HADOOP_CONF_DIR=
export YARN_CONF_DIR=

export TEMP_DIR=$SPARKCLR_HOME/Temp
[ ! -d "$TEMP_DIR" ] && mkdir "$TEMP_DIR"
export SAMPLES_DIR=$SPARKCLR_HOME/samples

echo "[run-samples.sh] JAVA_HOME=$JAVA_HOME"
echo "[run-samples.sh] SPARK_HOME=$SPARK_HOME"
echo "[run-samples.sh] SPARKCLR_HOME=$SPARKCLR_HOME"
echo "[run-samples.sh] SPARKCSV_JARS=$SPARKCSV_JARS"

echo "[run-samples.sh] sparkclr-submit.sh --exe SparkCLRSamples.exe $SAMPLES_DIR spark.local.dir $TEMP_DIR sparkclr.sampledata.loc $SPARKCLR_HOME/data $@"
"$SPARKCLR_HOME/scripts/sparkclr-submit.sh" --exe SparkCLRSamples.exe "$SAMPLES_DIR" spark.local.dir "$TEMP_DIR" sparkclr.sampledata.loc "$SPARKCLR_HOME/data" "$@"

# explicitly export the exitcode as a reminder for future changes
export exitcode=$?
exit $exitcode
