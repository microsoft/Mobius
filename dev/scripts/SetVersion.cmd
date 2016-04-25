@echo OFF
setlocal enabledelayedexpansion

IF "%1"=="" (goto :usage)
IF "%2"=="" (goto :usage)

set ProjectVersion=%1

@echo [SetVersion.cmd] ProjectVersion=%ProjectVersion

pushd %~dp0

@rem Windows 7/8/10 may not allow powershell scripts by default
powershell -Command Set-ExecutionPolicy -Scope CurrentUser Unrestricted

if "%2" == "examples" (
	@echo Updating version number in examples
	@rem update Mobius Nuget package version reference only in examples
	powershell -f SetSparkClrPackageVersion.ps1 -targetDir ..\..\examples -version %ProjectVersion% -nuspecDir ..\..\csharp -mode %2
) else if "%2" == "core" (
	@echo Updating version number in core artifacts (like pom, nuspec files)
	@rem update version in pom.xml
	pushd ..\..\scala
	@echo call mvn versions:set -DnewVersion=%ProjectVersion%
	call mvn versions:set -DnewVersion=%ProjectVersion%
	popd

	@rem update Mobius jar version reference 
	powershell -f SetSparkClrJarVersion.ps1 -targetDir ..\..\scripts -version %ProjectVersion%
	
	@rem update Moibus Nuget package version in nuspec file 
	powershell -f SetSparkClrPackageVersion.ps1 -targetDir ..\..\examples -version %ProjectVersion% -nuspecDir ..\..\csharp -mode %2
)

popd

goto :eof

:usage
@echo =============================================================
@echo.
@echo     %0 requires a version string as the only parameter. 
@echo     Example usage below -   
@echo         %0 1.5.200-preview-1 [core|examples]
@echo         						core - to update version in Mobius core artifacts (pom, nuspec files)
@echo         						examples - to update version in Mobius examples
@echo.
@echo =============================================================
