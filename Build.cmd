@setlocal
@echo OFF

SET CMDHOME=%~dp0
@REM Remove trailing backslash \
set CMDHOME=%CMDHOME:~0,-1%

set SPARKCLR_HOME=%CMDHOME%\run
@echo SPARKCLR_HOME=%SPARKCLR_HOME%

if NOT EXIST "%SPARKCLR_HOME%" mkdir "%SPARKCLR_HOME%"
if NOT EXIST "%SPARKCLR_HOME%\bin" mkdir "%SPARKCLR_HOME%\bin"
if NOT EXIST "%SPARKCLR_HOME%\data" mkdir "%SPARKCLR_HOME%\data"
if NOT EXIST "%SPARKCLR_HOME%\lib" mkdir "%SPARKCLR_HOME%\lib"
if NOT EXIST "%SPARKCLR_HOME%\samples" mkdir "%SPARKCLR_HOME%\samples"
if NOT EXIST "%SPARKCLR_HOME%\scripts" mkdir "%SPARKCLR_HOME%\scripts"

@echo Assemble SparkCLR Scala components
pushd "%CMDHOME%\scala"
call mvn.cmd package
@echo SparkCLR Scala binaries
copy /y target\*.jar "%SPARKCLR_HOME%\lib\"
popd

@REM Any .jar files under the lib directory will be copied to the staged runtime lib tree.
if EXIST "%CMDHOME%\lib" (
  @echo Copy extra jar library binaries
  FOR /F "tokens=*" %%G IN ('DIR /B /A-D /S %CMDHOME%\lib\*.jar') DO (
    @echo %%G
    copy /y "%%G" "%SPARKCLR_HOME%\lib\"
  )
)

@echo Assemble SparkCLR C# components
pushd "%CMDHOME%\csharp"
call Build.cmd
@echo SparkCLR C# binaries
copy /y Worker\Microsoft.Spark.CSharp\bin\Release\* "%SPARKCLR_HOME%\bin\"
@echo SparkCLR C# Samples binaries
copy /y Samples\Microsoft.Spark.CSharp\bin\Release\* "%SPARKCLR_HOME%\samples\"
@echo SparkCLR Samples data
copy /y Samples\Microsoft.Spark.CSharp\data\* "%SPARKCLR_HOME%\data\"
popd

@echo Assemble SparkCLR script components
pushd "%CMDHOME%\scripts"
copy /y *.cmd  "%SPARKCLR_HOME%\scripts\"
popd
