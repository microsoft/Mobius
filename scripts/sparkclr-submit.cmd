@echo off
setlocal enabledelayedexpansion
if "%SPARK_HOME%" == "" goto :sparkhomeerror
if "%JAVA_HOME%" == "" goto :javahomeerror
if "%SPARKCLR_HOME%" == "" goto :sparkclrhomeerror

if "%SPARK_CONF_DIR%" == "" (
	SET SPARK_CONF_DIR=%SPARK_HOME%\conf
)

call "%SPARK_HOME%\bin\load-spark-env.cmd"

rem Test that an argument was given
if "x%1"=="x" (
  goto :usage
)

set ASSEMBLY_DIR=%SPARK_HOME%\lib

for %%d in ("%ASSEMBLY_DIR%"\spark-assembly*hadoop*.jar) do (
  set SPARK_ASSEMBLY_JAR=%%d
)
if "%SPARK_ASSEMBLY_JAR%"=="0" (
  @echo [sparkclr-submit.cmd] Failed to find Spark assembly JAR.
  exit /b 1
)

set SPARKCLR_JAR=spark-clr_2.10-1.5.2-SNAPSHOT.jar
set SPARKCLR_CLASSPATH=%SPARKCLR_HOME%\lib\%SPARKCLR_JAR%
if not "%SPARKCSV_JARS%" == "" (
    SET SPARKCLR_CLASSPATH=%SPARKCLR_CLASSPATH%;%SPARKCSV_JARS%
)
set LAUNCH_CLASSPATH=%SPARK_ASSEMBLY_JAR%;%SPARKCLR_CLASSPATH%

if "%1"=="debug" (
  goto :debugmode
)

rem The launcher library prints the arguments to be submitted to spark-submit.cmd. So read all the output of the launcher into a variable.
set LAUNCHER_OUTPUT=%temp%\spark-class-launcher-output-%RANDOM%.txt
"%JAVA_HOME%\bin\java" -cp "%LAUNCH_CLASSPATH%" org.apache.spark.launcher.SparkCLRSubmitArguments %* > %LAUNCHER_OUTPUT%

if %ERRORLEVEL% NEQ 0 (
   goto :eof
)

for /f "tokens=*" %%i in (%LAUNCHER_OUTPUT%) do (
  set SPARK_ARGS=%%i
)

del %LAUNCHER_OUTPUT%

REM launches the Spark job with Spark-Submit.cmd
@echo [sparkclr-submit.cmd] Command to run %SPARK_ARGS%
"%SPARK_HOME%/bin/spark-submit.cmd" %SPARK_ARGS%

goto :eof

:debugmode
"%JAVA_HOME%\bin\java" -cp "%LAUNCH_CLASSPATH%" org.apache.spark.deploy.csharp.CSharpRunner debug
goto :eof

:sparkhomeerror
	@echo [sparkclr-submit.cmd] Error - SPARK_HOME environment variable is not set
	@echo [sparkclr-submit.cmd] Note that SPARK_HOME environment variable should not have trailing \
	goto :eof
	
:javahomeerror
	@echo [sparkclr-submit.cmd] Error - JAVA_HOME environment variable is not set
	@echo [sparkclr-submit.cmd] Note that JAVA_HOME environment variable should not have trailing \
	goto :eof
	
:sparkclrhomeerror
	@echo [sparkclr-submit.cmd] Error - SPARKCLR_HOME environment variable is not set
	@echo [sparkclr-submit.cmd] SPARKCLR_HOME need to be set to the folder path for csharp-spark*.jar
	@echo [sparkclr-submit.cmd] Note that SPARKCLR_HOME environment variable should not have trailing \
	goto :eof

:usage
	@echo Error - usage error.
	@echo Correct usage is as follows
	@echo Example 1:
	@echo sparkclr-submit.cmd [--verbose] [--master local] [--deploy-mode client] [--name testapp] --exe csdriver.exe c:\sparkclrapp\driver arg1 arg2 arg3
	@echo Example 2:
	@echo sparkclr-submit.cmd [--verbose] [--master local] [--deploy-mode client] [--name testapp] --exe csdriver.exe c:\sparkclrapp\driver.zip arg1 arg2 arg3
	@echo Example 3:
	@echo sparkclr-submit.cmd [--verbose] --master spark://host:port --deploy-mode cluster [--name testapp] --exe csdriver.exe --remote-sparkclr-jar hdfs://path/to/spark-clr-1.4.1-SNAPSHOT.jar hdfs://path/to/driver.zip arg1 arg2 arg3
