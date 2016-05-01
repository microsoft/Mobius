@setlocal
@echo OFF

if "%1" == "csharp" set buildCSharp=true

SET CMDHOME=%~dp0
@REM Remove trailing backslash \
set CMDHOME=%CMDHOME:~0,-1%

@rem check prerequisites
call "%CMDHOME%\localmode\precheck.cmd"

if %precheck% == "bad" (goto :eof)

@rem Windows 7/8/10 may not allow powershell scripts by default
powershell -Command Set-ExecutionPolicy -Scope CurrentUser Unrestricted

@rem download build tools
pushd "%CMDHOME%"
powershell -f localmode\downloadtools.ps1 build
call tools\updatebuildtoolenv.cmd
popd

set SPARKCLR_HOME=%CMDHOME%\runtime
@echo SPARKCLR_HOME=%SPARKCLR_HOME%

if defined buildCSharp goto :buildCSharp

if EXIST "%SPARKCLR_HOME%" (
    @echo Delete existing %SPARKCLR_HOME% ...
    rd /s /q "%SPARKCLR_HOME%"
)

if NOT EXIST "%SPARKCLR_HOME%" mkdir "%SPARKCLR_HOME%"
if NOT EXIST "%SPARKCLR_HOME%\bin" mkdir "%SPARKCLR_HOME%\bin"
if NOT EXIST "%SPARKCLR_HOME%\data" mkdir "%SPARKCLR_HOME%\data"
if NOT EXIST "%SPARKCLR_HOME%\lib" mkdir "%SPARKCLR_HOME%\lib"
if NOT EXIST "%SPARKCLR_HOME%\samples" mkdir "%SPARKCLR_HOME%\samples"

@echo Assemble Mobius Scala components
pushd "%CMDHOME%\..\scala"

@rem clean the target directory first
call mvn.cmd %MVN_QUIET% clean

@rem
@rem Note: Shade-plugin helps creates an uber-package to simplify running samples during CI;
@rem however, it breaks debug mode in IntellJ. So enable shade-plugin
@rem only in build.cmd to create the uber-package.
@rem
copy /y pom.xml %temp%\pom.xml.original
powershell -f ..\build\localmode\patchpom.ps1 pom.xml 
copy /y pom.xml %temp%\pom.xml.patched

IF "%APPVEYOR_REPO_TAG%" == "true" (goto :sign)

    @rem build the package
    call mvn.cmd %MVN_QUIET% package -Puber-jar
    goto :mvndone

:sign

    @rem
    @rem prepare signing only when APPVEYOR_REPO_TAG is available
    @rem
    
    gpg2 --batch --yes --import ..\build\data\private_token.asc
    gpg2 --batch --yes --import ..\build\data\public_token.asc
    
    pushd %APPDATA%\gnupg 
    del /q trustdb.gpg 
    popd
    gpg2 --batch --yes --import-ownertrust < ..\build\data\ownertrustblob.txt
    
    gpg2 --list-key
    
    @rem build the package, sign, deploy to maven central
    call mvn %MVN_QUIET% clean deploy -Puber-jar -DdoSign=true -DdoRelease=true

:mvndone

@rem
@rem After uber package is created, restore Pom.xml
@rem
copy /y %temp%\pom.xml.original pom.xml

if %ERRORLEVEL% NEQ 0 (
  @echo Build Mobius Scala components failed, stop building.
  popd
  goto :eof
)

@echo Mobius Scala binaries
@rem copy non-uber jar to runtime\lib folder
powershell -f ..\build\copyjar.ps1
popd

@REM Any .jar files under the lib directory will be copied to the staged runtime lib tree.
if EXIST "%CMDHOME%\lib" (
  @echo Copy extra jar library binaries
  FOR /F "tokens=*" %%G IN ('DIR /B /A-D /S %CMDHOME%\lib\*.jar') DO (
    @echo %%G
    copy /y "%%G" "%SPARKCLR_HOME%\lib\"
  )
)

:buildCSharp
@echo Assemble Mobius C# components
pushd "%CMDHOME%\..\csharp"

@rem clean any possible previous build first
call Clean.cmd
call Build.cmd

if %ERRORLEVEL% NEQ 0 (
  @echo Build Mobius C# components failed, stop building.
  popd
  goto :eof
)

@echo Mobius C# binaries
copy /y Worker\Microsoft.Spark.CSharp\bin\Release\* "%SPARKCLR_HOME%\bin\"

@echo Mobius C# Samples binaries
@rem need to include CSharpWorker.exe.config in samples folder
copy /y Worker\Microsoft.Spark.CSharp\bin\Release\* "%SPARKCLR_HOME%\samples\"
copy /y Samples\Microsoft.Spark.CSharp\bin\Release\* "%SPARKCLR_HOME%\samples\"

@echo Mobius Samples data
copy /y Samples\Microsoft.Spark.CSharp\data\* "%SPARKCLR_HOME%\data\"
popd

@echo Download external dependencies
pushd "%CMDHOME%"
set DEPENDENCIES_DIR=dependencies
if NOT EXIST "%DEPENDENCIES_DIR%" mkdir %DEPENDENCIES_DIR%
set DEPENDENCIES_HOME=%CMDHOME%\%DEPENDENCIES_DIR%
powershell -f localmode\downloadtools.ps1 dependencies
@echo Assemble dependencies
xcopy /e /y "%DEPENDENCIES_HOME%"  "%SPARKCLR_HOME%\dependencies\"

@echo Assemble Mobius examples
pushd "%CMDHOME%\..\examples"
call Clean.cmd
call Build.cmd

if %ERRORLEVEL% NEQ 0 (
  @echo Build Mobius C# examples failed, stop building.
  popd
  goto :eof
)

set EXAMPLES_HOME=%CMDHOME%\examples
@echo set EXAMPLES_HOME=%EXAMPLES_HOME%

if EXIST "%EXAMPLES_HOME%" (
    @echo Delete existing %EXAMPLES_HOME% ...
    rd /s /q "%EXAMPLES_HOME%"
)
if NOT EXIST "%EXAMPLES_HOME%" mkdir "%EXAMPLES_HOME%"

set CURRDIR=%cd%
for /f "delims=" %%D in ('dir /b /s bin') do call :copyexamples %%D
goto :copyscripts

:copyexamples
    set EXAMPLES_SRC=%1
    set EXAMPLES_TARGET=%1
    call set EXAMPLES_TARGET=%%EXAMPLES_TARGET:%CURRDIR%=%EXAMPLES_HOME%%%
    set EXAMPLES_TARGET=%EXAMPLES_TARGET:~0,-4%

    @echo mkdir %EXAMPLES_TARGET%
    if NOT EXIST "%EXAMPLES_TARGET%" mkdir "%EXAMPLES_TARGET%"

    REM 1. Copy dependencies from %SPARKCLR_HOME%\bin to use latest Mobius binaries
    xcopy /y "%SPARKCLR_HOME%\bin\*" "%EXAMPLES_TARGET%"
    REM 2. copy Examples APPs
    xcopy /d /y "%EXAMPLES_SRC%\Release" "%EXAMPLES_TARGET%"

    goto :eof

:copyscripts
popd

@echo Assemble Mobius script components
xcopy /e /y "%CMDHOME%\..\scripts"  "%SPARKCLR_HOME%\scripts\"

@echo Make distribution
pushd "%CMDHOME%"
if not exist ".\target" (mkdir .\target)

if not defined ProjectVersion (
    powershell -f .\localmode\zipdir.ps1 -dir "%SPARKCLR_HOME%" -target ".\target\runtime.zip"
    goto :distdone
)

set SPARKCLR_NAME=spark-clr_2.10-%ProjectVersion%
@echo "%SPARKCLR_HOME%

@rem copy samples to top-level folder before zipping
@echo move /Y "%SPARKCLR_HOME%\samples "%CMDHOME%"
move /Y %SPARKCLR_HOME%\samples %CMDHOME%
@echo move /Y "%SPARKCLR_HOME%\data" "%CMDHOME%\samples"
move /Y %SPARKCLR_HOME%\data %CMDHOME%\samples

@rem copy release info
@echo copy /Y "%CMDHOME%\..\notes\mobius-release-info.md"
copy /Y "%CMDHOME%\..\notes\mobius-release-info.md"

@rem Create the zip file
@echo 7z a .\target\%SPARKCLR_NAME%.zip runtime examples samples mobius-release-info.md
7z a .\target\%SPARKCLR_NAME%.zip runtime examples samples mobius-release-info.md

:distdone
popd
