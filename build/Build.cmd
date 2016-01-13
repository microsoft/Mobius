@echo OFF
setlocal

@rem check prerequisites
call .\localmode\precheck.cmd

if %precheck% == "bad" (goto :eof)

@rem Windows 7/8/10 may not allow powershell scripts by default
powershell -Command Set-ExecutionPolicy -Scope CurrentUser Unrestricted

@rem download build tools
pushd %~dp0
powershell -f localmode\downloadtools.ps1 build
call tools\updatebuildtoolenv.cmd
popd

SET CMDHOME=%~dp0
@REM Remove trailing backslash \
set CMDHOME=%CMDHOME:~0,-1%

set SPARKCLR_HOME=%CMDHOME%\run
@echo SPARKCLR_HOME=%SPARKCLR_HOME%

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
call mvn.cmd clean

@rem
@rem Note: Shade-plugin helps creates an uber-package to simplify SparkCLR job submission;
@rem however, it breaks debug mode in IntellJ. So enable shade-plugin
@rem only in build.cmd to create the uber-package.
@rem
copy /y pom.xml %temp%\pom.xml.original
powershell -f ..\build\localmode\patchpom.ps1 pom.xml 
copy /y pom.xml %temp%\pom.xml.patched

IF NOT "%APPVEYOR_REPO_TAG%" == "true" (goto :nosign)
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
    
    @rem ProjectVersion is set in downloadtools.ps1, based on AppVeyor-Repo-Tag
    if DEFINED ProjectVersion (
      set SPARKCLR_NAME=spark-clr_2.10-%ProjectVersion%
      echo call mvn versions:set -DnewVersion=%ProjectVersion%
      call mvn versions:set -DnewVersion=%ProjectVersion%
    )

    echo SPARKCLR_NAME=%SPARKCLR_NAME%
    
    @rem build the package, sign, deploy to maven central
    call mvn clean deploy -Puber-jar -DdoSign=true -DdoRelease=true
    goto :mvndone

:nosign

    @rem build the package
    call mvn.cmd package -Puber-jar

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
rem need to include CSharpWorker.exe.config in samples folder
copy /y Worker\Microsoft.Spark.CSharp\bin\Release\* "%SPARKCLR_HOME%\samples\"
copy /y Samples\Microsoft.Spark.CSharp\bin\Release\* "%SPARKCLR_HOME%\samples\"

@echo SparkCLR Samples data
copy /y Samples\Microsoft.Spark.CSharp\data\* "%SPARKCLR_HOME%\data\"
popd

@echo Assemble SparkCLR script components
xcopy /e /y "%CMDHOME%\..\scripts"  "%SPARKCLR_HOME%\scripts\"

@echo Make distribution
pushd %~dp0
if not exist ".\target" (mkdir .\target)

if not defined SPARKCLR_NAME (
    powershell -f .\localmode\zipdir.ps1 -dir "%SPARKCLR_HOME%" -target ".\target\run.zip"
    goto :distdone
)

set TARGET_FILE=.\run\scripts\sparkclr-submit.cmd
@rem update sparkclr version in sparkclr-submit.cmd file
powershell -NoProfile -ExecutionPolicy Bypass -Command "((Get-Content %TARGET_FILE%) -replace '\(set SPARKCLR_JAR=.*\)', '(set SPARKCLR_JAR=%SPARKCLR_NAME%.jar)') | Set-Content %TARGET_FILE% -force"

xcopy /e /y ..\examples  .\examples\
@rem update sparkclr nuget package reference versions in *.csproj and packages.config under .\examples
powershell -f .\localmode\setversion.ps1 -dir .\examples -version %ProjectVersion%

@rem Create the zip file
@echo 7z a .\target\%SPARKCLR_NAME%.zip run localmode .\examples
7z a .\target\%SPARKCLR_NAME%.zip run localmode examples

:distdone
popd
