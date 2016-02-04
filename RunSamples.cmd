@echo OFF
setlocal enabledelayedexpansion

set VERBOSE=
set INTERACTIVE=

:argsloop

if "%1" == "" (
    rem - no more arguments. 
) else (
    rem - check each argument
    if "%1" == "--verbose" (
        set VERBOSE="verbose"
        @echo [RunSamples.cmd] VERBOSE is !VERBOSE!
    )
    if "%1" == "-i" (
        set INTERACTIVE="interactive"
        @echo [RunSamples.cmd] Interactive mode is ON.
    )
    rem - shift the arguments and examine %1 again
    shift
    goto argsloop
)

@rem check prerequisites
call precheck.cmd
if %precheck% == "bad" (goto :eof)

@rem 
@rem setup Hadoop and Spark versions
@rem
set SPARK_VERSION=1.5.2
set HADOOP_VERSION=2.6
@echo [RunSamples.cmd] SPARK_VERSION=%SPARK_VERSION%, HADOOP_VERSION=%HADOOP_VERSION%

@rem Windows 7/8/10 may not allow powershell scripts by default
powershell -Command Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy Unrestricted

@rem download runtime dependencies
pushd %~dp0
powershell -f downloadtools.ps1 run !VERBOSE!
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

@echo [RunSamples.cmd] JAVA_HOME=%JAVA_HOME%
@echo [RunSamples.cmd] SPARK_HOME=%SPARK_HOME%
@echo [RunSamples.cmd] SPARKCLR_HOME=%SPARKCLR_HOME%
@echo [RunSamples.cmd] SPARKCSV_JARS=%SPARKCSV_JARS%

pushd %SPARKCLR_HOME%\scripts

if !INTERACTIVE! == "interactive" (
	@echo [RunSamples.cmd] call sparkclr-shell.cmd %*
	call sparkclr-shell.cmd
) else (
	@echo [RunSamples.cmd] call sparkclr-submit.cmd --exe SparkCLRSamples.exe %SAMPLES_DIR% spark.local.dir %TEMP_DIR% sparkclr.sampledata.loc %SPARKCLR_HOME%\data %*
	call sparkclr-submit.cmd --exe SparkCLRSamples.exe %SAMPLES_DIR% spark.local.dir %TEMP_DIR% sparkclr.sampledata.loc %SPARKCLR_HOME%\data %*
)

popd
