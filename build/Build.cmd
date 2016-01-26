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

@echo Assemble SparkCLR Scala components
pushd "%CMDHOME%\..\scala"

@rem clean the target directory first
call mvn.cmd %MVN_QUIET% clean

@rem
@rem Note: Shade-plugin helps creates an uber-package to simplify SparkCLR job submission;
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
  @echo Build SparkCLR Scala components failed, stop building.
  popd
  goto :eof
)

@echo SparkCLR Scala binaries
copy /y target\spark*.jar "%SPARKCLR_HOME%\lib\"
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
@echo Assemble SparkCLR C# components
pushd "%CMDHOME%\..\csharp"

@rem clean any possible previous build first
call Clean.cmd
call Build.cmd

if %ERRORLEVEL% NEQ 0 (
  @echo Build SparkCLR C# components failed, stop building.
  popd
  goto :eof
)

@echo SparkCLR C# binaries
copy /y Worker\Microsoft.Spark.CSharp\bin\Release\* "%SPARKCLR_HOME%\bin\"

@echo SparkCLR C# Samples binaries
@rem need to include CSharpWorker.exe.config in samples folder
copy /y Worker\Microsoft.Spark.CSharp\bin\Release\* "%SPARKCLR_HOME%\samples\"
copy /y Samples\Microsoft.Spark.CSharp\bin\Release\* "%SPARKCLR_HOME%\samples\"

@echo SparkCLR Samples data
copy /y Samples\Microsoft.Spark.CSharp\data\* "%SPARKCLR_HOME%\data\"
popd

@echo Assemble SparkCLR script components
xcopy /e /y "%CMDHOME%\..\scripts"  "%SPARKCLR_HOME%\scripts\"

@echo Make distribution
pushd "%CMDHOME%"
if not exist ".\target" (mkdir .\target)

if not defined ProjectVersion (
    powershell -f .\localmode\zipdir.ps1 -dir "%SPARKCLR_HOME%" -target ".\target\runtime.zip"
    goto :distdone
)

set SPARKCLR_NAME=spark-clr_2.10-%ProjectVersion%

@rem Create the zip file
@echo 7z a .\target\%SPARKCLR_NAME%.zip runtime localmode ..\examples
7z a .\target\%SPARKCLR_NAME%.zip runtime localmode ..\examples

:distdone
popd
