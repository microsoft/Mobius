@echo OFF
setlocal enabledelayedexpansion

SET CMDHOME=%~dp0
@REM Remove trailing backslash \
set CMDHOME=%CMDHOME:~0,-1%

set VERBOSE=
set USER_EXE=

:argsloop

if "%1" == "" (
    rem - no more arguments. 
) else (
    rem - check each argument
    if "%1" == "--verbose" (
        set VERBOSE="verbose"
        @echo [RunSamples.cmd] VERBOSE is !VERBOSE!
    )

    @rem TODO: this check will fail if "--exe" only exists in the argument list of user application.
    if "%1" == "--exe" (
        set USER_EXE="true"
        @echo [RunSamples.cmd] Run user specified application, instead of SparkCLR samples.
    )

    rem - shift the arguments and examine %1 again
    shift
    goto argsloop
)

pushd "%CMDHOME%"
@echo [RunSamples.cmd] CWD=
@cd

@rem check prerequisites
call precheck.cmd
if "%precheck%" == "bad" (goto :EOF)

@rem 
@rem setup Hadoop and Spark versions
@rem
set SPARK_VERSION=1.6.1
set HADOOP_VERSION=2.6
@echo [RunSamples.cmd] SPARK_VERSION=%SPARK_VERSION%, HADOOP_VERSION=%HADOOP_VERSION%

@rem Windows 7/8/10 may not allow powershell scripts by default
powershell -Command Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy Unrestricted

@rem download runtime dependencies
pushd "%CMDHOME%"
powershell -f downloadtools.ps1 run !VERBOSE!
@echo [RunSamples.cmd] UpdateRuntime.cmd
type ..\tools\updateruntime.cmd
call ..\tools\updateruntime.cmd
popd

if defined ProjectVersion (
    set SPARKCLR_JAR=spark-clr_2.10-%ProjectVersion%.jar
)

set SPARKCLR_HOME=%CMDHOME%\..\runtime
set SPARKCSV_JARS=

@rem RunSamples.cmd is in local mode, should not load Hadoop or Yarn cluster config. Disable Hadoop/Yarn conf dir.
set HADOOP_CONF_DIR=
set YARN_CONF_DIR=

set TEMP_DIR=%SPARKCLR_HOME%\Temp
if NOT EXIST "%TEMP_DIR%" mkdir "%TEMP_DIR%"
set SAMPLES_DIR=%SPARKCLR_HOME%\samples

@echo [RunSamples.cmd] SPARKCLR_JAR=%SPARKCLR_JAR%
@echo [RunSamples.cmd] JAVA_HOME=%JAVA_HOME%
@echo [RunSamples.cmd] SPARK_HOME=%SPARK_HOME%
@echo [RunSamples.cmd] SPARKCLR_HOME=%SPARKCLR_HOME%
@echo [RunSamples.cmd] SPARKCSV_JARS=%SPARKCSV_JARS%

pushd "%SPARKCLR_HOME%\scripts"
@echo [RunSamples.cmd] CWD=
@cd
@dir /s "%SPARKCLR_HOME%"

if "!USER_EXE!"=="" (
    @echo [RunSamples.cmd] call sparkclr-submit.cmd --exe SparkCLRSamples.exe %SAMPLES_DIR% spark.local.dir %TEMP_DIR% sparkclr.sampledata.loc %SPARKCLR_HOME%\data %*
    call sparkclr-submit.cmd --exe SparkCLRSamples.exe %SAMPLES_DIR% spark.local.dir %TEMP_DIR% sparkclr.sampledata.loc %SPARKCLR_HOME%\data %*
) else (
    @echo [RunSamples.cmd] call sparkclr-submit.cmd %*
    call sparkclr-submit.cmd %*
)

@if ERRORLEVEL 1 GOTO :ErrorStop

@GOTO :EOF

:ErrorStop
set RC=%ERRORLEVEL%
@echo ===== sparkclr-submit FAILED error %RC% =====
exit /B %RC%

:EOF
