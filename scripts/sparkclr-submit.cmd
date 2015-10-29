@echo off
setlocal enabledelayedexpansion
if "%SPARK_HOME%" == "" goto :sparkhomeerror
if "%JAVA_HOME%" == "" goto :javahomeerror
if "%SPARKCLR_HOME%" == "" goto :sparkclrhomeerror

if "%SPARK_CONF_DIR%" == "" (
	SET SPARK_CONF_DIR=%SPARK_HOME%\conf
)

call %SPARK_HOME%\bin\load-spark-env.cmd

rem Test that an argument was given
if "x%1"=="x" (
  goto :usage
)

set ASSEMBLY_DIR=%SPARK_HOME%\lib

for %%d in (%ASSEMBLY_DIR%\spark-assembly*hadoop*.jar) do (
  set SPARK_ASSEMBLY_JAR=%%d
)
if "%SPARK_ASSEMBLY_JAR%"=="0" (
  echo Failed to find Spark assembly JAR.
  exit /b 1
)

set SPARKCLR_JAR=csharp-spark-1.4.1-SNAPSHOT.jar
set SPARKCLR_CLASSPATH=%SPARKCLR_HOME%\lib\%SPARKCLR_JAR%
set LAUNCH_CLASSPATH=%SPARK_ASSEMBLY_JAR%;%SPARKCLR_CLASSPATH%
set SPARKCLR_SUBMIT_CLASS=org.apache.spark.deploy.csharp.SparkCLRSubmit
set SPARK_SUBMIT_CLASS=org.apache.spark.deploy.SparkSubmit
set JARS=%SPARKCLR_CLASSPATH%
if not "%SPARKCSV_JARS%" == "" (
	SET JARS=%JARS%,%SPARKCSV_JARS%
)
if not "%CSHARPSPARK_APP_JARS%" == "" (
	SET JARS=%JARS%,%CSHARPSPARK_APP_JARS%
)

if "%1"=="debug" (
  goto :debugmode
)

rem The launcher library prints the command to be executed in a single line suitable for being
rem executed by the batch interpreter. So read all the output of the launcher into a variable.
set LAUNCHER_OUTPUT=%temp%\spark-class-launcher-output-%RANDOM%.txt
%JAVA_HOME%\bin\java -cp %LAUNCH_CLASSPATH% org.apache.spark.launcher.Main %SPARK_SUBMIT_CLASS% --jars %JARS% --class %SPARKCLR_SUBMIT_CLASS% %* > %LAUNCHER_OUTPUT%

REM *********************************************************************************
REM ** TODO ** - replace the following sections in the script with a functionality that is implemented in scala 
REM ** TODO ** - that will call org.apache.spark.launcher.Main, perform class name substituition, do classpath prefixing and generate the command to run
REM Following sections are simply a hack to leverage existing Spark artifacts - this will also help keeping CSharpSpark aligned with Spark's approach for arg parsing etc.
REM Following block replaces SparkSubmit with SparkCLRSubmit
REM *********************************************************************************  
set LAUNCHER_OUTPUT_TEMP=sparkclr-submit-temp.txt
for /f "tokens=* delims= " %%A in ( '"type %LAUNCHER_OUTPUT%"') do (
SET originalstring=%%A
SET modifiedstring=!originalstring:%SPARK_SUBMIT_CLASS%=%SPARKCLR_SUBMIT_CLASS%!

echo !modifiedstring! >> %LAUNCHER_OUTPUT_TEMP%
)

del %LAUNCHER_OUTPUT%
REM *********************************************************************************

REM *********************************************************************************
REM Following block prefixes classpath with SPARKCLR_JAR
REM *********************************************************************************
set LAUNCHER_OUTPUT_TEMP2=sparkclr-submit-temp2.txt
set CLASSPATH_SUBSTRING=-cp "
set UPDATED_CLASSPATH_SUBSTRING=-cp "%SPARKCLR_CLASSPATH%;
for /f "tokens=* delims= " %%A in ( '"type %LAUNCHER_OUTPUT_TEMP%"') do (
SET originalstring2=%%A
SET modifiedstring2=!originalstring2:%CLASSPATH_SUBSTRING%=%UPDATED_CLASSPATH_SUBSTRING%!

echo !modifiedstring2! >> %LAUNCHER_OUTPUT_TEMP2%
)

del %LAUNCHER_OUTPUT_TEMP%
REM *********************************************************************************


for /f "tokens=*" %%i in (%LAUNCHER_OUTPUT_TEMP2%) do (
  set SPARK_CMD=%%i
)

del %LAUNCHER_OUTPUT_TEMP2%
REM launches the Spark job with SparkCLRSubmit as the Main class
echo Command to run %SPARK_CMD%
%SPARK_CMD%

goto :eof

:debugmode
%JAVA_HOME%\bin\java -cp %LAUNCH_CLASSPATH% org.apache.spark.deploy.csharp.CSharpRunner debug
goto :eof

:sparkhomeerror
	@echo Error - SPARK_HOME environment variable is not set
	@echo Note that SPARK_HOME environment variable should not have trailing \
	goto :eof
	
:javahomeerror
	@echo Error - JAVA_HOME environment variable is not set
	@echo Note that JAVA_HOME environment variable should not have trailing \
	goto :eof
	
:sparkclrhomeerror
	@echo Error - SPARKCLR_HOME environment variable is not set
	@echo SPARKCLR_HOME need to be set to the folder path for csharp-spark*.jar
	@echo Note that SPARKCLR_HOME environment variable should not have trailing \
	goto :eof

:usage
	@echo Error - usage error.
	@echo Correct usage is as follows
	@echo sparkclr-submit.cmd [--verbose] [--master local] [--name testapp] d:\SparkCLRHome\lib\spark-clr-1.4.1-SNAPSHOT.jar c:\sparkclrapp\driver\csdriver.exe arg1 arg2 arg3
