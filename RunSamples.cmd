@echo OFF
setlocal

@rem check prerequisites
call precheck.cmd
if %precheck% == "bad" (goto :eof)

@rem 
@rem setup Hadoop and Spark versions
@rem
set SPARK_VERSION=1.4.1
set HADOOP_VERSION=2.6
@echo SPARK_VERSION=%SPARK_VERSION%
@echo HADOOP_VERSION=%HADOOP_VERSION%

@rem Windows 7/8/10 may not allow powershell scripts by default
powershell -Command Set-ExecutionPolicy Unrestricted

@rem download runtime dependencies
pushd %~dp0
powershell -f downloadtools.ps1 run
call tools\updateruntime.cmd
popd

SET CMDHOME=%~dp0
@REM Remove trailing backslash \
set CMDHOME=%CMDHOME:~0,-1%

set SPARKCLR_HOME=%CMDHOME%\run
set SPARKCSV_JARS=

@rem RunSamples.cmd is in local mode, should not load Hadoop or Yarn cluster config. Disable Hadoop/Yarn conf dir.
set HADOOP_CONF_DIR=
set YARN_CONF_DIR=

set TEMP_DIR=%SPARKCLR_HOME%\Temp
if NOT EXIST "%TEMP_DIR%" mkdir "%TEMP_DIR%"
set SAMPLES_DIR=%SPARKCLR_HOME%\samples

@echo JAVA_HOME=%JAVA_HOME%
@echo SPARK_HOME=%SPARK_HOME%
@echo SPARKCLR_HOME=%SPARKCLR_HOME%
@echo SPARKCSV_JARS=%SPARKCSV_JARS%

pushd %SPARKCLR_HOME%
@cd

@echo ON

call %SPARKCLR_HOME%\scripts\sparkclr-submit.cmd --exe SparkCLRSamples.exe %SAMPLES_DIR% spark.local.dir %TEMP_DIR% sparkclr.sampledata.loc %SPARKCLR_HOME%\data %*

@echo OFF
popd
