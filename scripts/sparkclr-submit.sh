#!/bin/bash

function spark_home_error() {
	echo "[sparkclr-submit.sh] Error - SPARK_HOME environment variable is not export"
	echo "[sparkclr-submit.sh] Note that SPARK_HOME environment variable should not have trailing /"
	exit 1
}
	
function java_home_error() {
	echo "[sparkclr-submit.sh] Error - JAVA_HOME environment variable is not set"
	echo "[sparkclr-submit.sh] Note that JAVA_HOME environment variable should not have trailing /"
	exit 1
}
	
function sparkclr_home_error() {
	echo "[sparkclr-submit.sh] Error - SPARKCLR_HOME environment variable is not set"
	echo "[sparkclr-submit.sh] SPARKCLR_HOME need to be set to the folder path for csharp-spark*.jar"
	echo "[sparkclr-submit.sh] Note that SPARKCLR_HOME environment variable should not have trailing /"
	exit 1
}

function usage() {
	echo "Error - usage error."
	echo "Correct usage is as follows"
	echo "Example 1:"
	echo "sparkclr-submit.sh [--verbose] [--master local] [--deploy-mode client] [--name testapp] --exe csdriver.exe sparkclrapp/driver arg1 arg2 arg3"
	echo "Example 2:"
	echo "sparkclr-submit.sh [--verbose] [--master local] [--deploy-mode client] [--name testapp] --exe csdriver.exe sparkclrapp/driver.zip arg1 arg2 arg3"
	echo "Example 3:"
	echo "sparkclr-submit.sh [--verbose] --master spark://host:port --deploy-mode cluster [--name testapp] --exe csdriver.exe --remote-sparkclr-jar --remote-sparkclr-jar hdfs://path/to/spark-clr_2.10-1.6.1-SNAPSHOT.jar hdfs://path/to/driver.zip arg1 arg2 arg3"
}

[ "$SPARK_HOME" = "" ] && spark_home_error
[ "$JAVA_HOME" = "" ] && java_home_error
[ "$SPARKCLR_HOME" = "" ] && sparkclr_home_error

[ "%SPARK_CONF_DIR%" = "" ] && export SPARK_CONF_DIR="$SPARK_HOME/conf"

. "$SPARK_HOME/bin/load-spark-env.sh"

# Test that an argument was given
[ $# -le 1 ] && usage

export SPARK_JARS_DIR="$SPARK_HOME/jars"

if [ ! -d "$SPARK_JARS_DIR" ];
then
  echo "[sparkclr-submit.sh] Failed to find Spark jars directory."
  echo "[sparkclr-submit.sh] You need to build Spark before running this program."
  exit 1
fi

export SPARK_JARS_CLASSPATH="$SPARK_JARS_DIR\*"

export SPARKCLR_JAR=spark-clr_2.11-2.0.000-SNAPSHOT.jar
export SPARKCLR_CLASSPATH="$SPARKCLR_HOME/lib/$SPARKCLR_JAR"
# SPARKCLR_DEBUGMODE_EXT_JARS environment variable is used to specify external dependencies to use in debug mode
[ ! "$SPARKCLR_DEBUGMODE_EXT_JARS" = "" ] && export SPARKCLR_CLASSPATH="$SPARKCLR_CLASSPATH:$SPARKCLR_DEBUGMODE_EXT_JARS"
export LAUNCH_CLASSPATH="$SPARKCLR_CLASSPATH:$SPARK_JARS_CLASSPATH"
echo "[sparkclr-submit.sh] LAUNCH_CLASSPATH=$LAUNCH_CLASSPATH"

if [ $1 = "debug" ];
then
  "$JAVA_HOME/bin/java" -cp "$LAUNCH_CLASSPATH" org.apache.spark.deploy.csharp.CSharpRunner debug
else

  # The launcher library prints the arguments to be submitted to spark-submit.sh. So read all the output of the launcher into a variable.
  export SPARK_ARGS=`"$JAVA_HOME/bin/java" -cp "$LAUNCH_CLASSPATH" org.apache.spark.launcher.SparkCLRSubmitArguments "$@"`
  
  # launches the Spark job with spark-submit.sh
  echo "[sparkclr-submit.sh] Command to run $SPARK_ARGS"
  "$SPARK_HOME/bin/spark-submit" $SPARK_ARGS
fi
