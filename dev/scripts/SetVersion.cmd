@echo OFF
setlocal enabledelayedexpansion

IF "%1"=="" (goto :usage)

set ProjectVersion=%1

@echo [SetVersion.cmd] ProjectVersion=%ProjectVersion

pushd %~dp0

@rem update version in pom.xml
pushd ..\..\scala
@echo call mvn versions:set -DnewVersion=%ProjectVersion%
call mvn versions:set -DnewVersion=%ProjectVersion%
popd

@rem Windows 7/8/10 may not allow powershell scripts by default
powershell -Command Set-ExecutionPolicy -Scope CurrentUser Unrestricted

@rem update SparkClr Nuget package version reference 
powershell -f SetSparkClrPackageVersion.ps1 -targetDir ..\..\examples -version %ProjectVersion% -nuspecDir ..\..\csharp

@rem update SparkClr jar version reference 
powershell -f SetSparkClrJarVersion.ps1 -targetDir ..\..\scripts -version %ProjectVersion%

popd

goto :eof

:usage
@echo =============================================================
@echo.
@echo     %0 requires a version string as the only parameter. 
@echo     Example usage below -   
@echo         %0 1.5.200-preview-1
@echo.
@echo =============================================================
