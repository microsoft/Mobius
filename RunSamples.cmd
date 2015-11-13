@setlocal
@echo OFF

SET CMDHOME=%~dp0
@REM Remove trailing backslash \
set CMDHOME=%CMDHOME:~0,-1%

set SPARKCLR_HOME=%CMDHOME%\run
set SPARKCSV_JARS=

set TEMP_DIR=%SPARKCLR_HOME%\Temp
if NOT EXIST "%TEMP_DIR%" mkdir "%TEMP_DIR%"
set SAMPLES_DIR=%SPARKCLR_HOME%\samples

@echo JAVA_HOME=%JAVA_HOME%
@echo SPARK_HOME=%SPARK_HOME%
@echo SPARKCLR_HOME=%SPARKCLR_HOME%
@echo SPARKCSV_JARS=%SPARKCSV_JARS%

cd %SPARKCLR_HOME%
@cd

@echo ON

%SPARKCLR_HOME%\scripts\sparkclr-submit.cmd --exe SparkCLRSamples.exe %SAMPLES_DIR% spark.local.dir %TEMP_DIR% sparkclr.sampledata.loc %SPARKCLR_HOME%\data
