@setlocal
@ECHO off

rem
rem Copyright (c) Microsoft. All rights reserved.
rem Licensed under the MIT license. See LICENSE file in the project root for full license information.
rem

SET CMDHOME=%~dp0
@REM Remove trailing backslash \
set CMDHOME=%CMDHOME:~0,-1%

@REM Set msbuild location.
SET VisualStudioVersion=12.0
if EXIST "%VS140COMNTOOLS%" SET VisualStudioVersion=14.0

SET MSBUILDEXEDIR=%programfiles(x86)%\MSBuild\%VisualStudioVersion%\Bin
if NOT EXIST "%MSBUILDEXEDIR%\." SET MSBUILDEXEDIR=%programfiles%\MSBuild\%VisualStudioVersion%\Bin

SET MSBUILDEXE=%MSBUILDEXEDIR%\MSBuild.exe
SET MSBUILDOPT=/verbosity:minimal

if "%builduri%" == "" set builduri=Build.cmd

cd "%CMDHOME%"
@cd

set PROJ_NAME=Examples
set PROJ=%CMDHOME%\%PROJ_NAME%.sln

@echo ===== Building %PROJ% =====

@echo Restore NuGet packages ===================
SET STEP=NuGet-Restore

nuget restore "%PROJ%"

@if ERRORLEVEL 1 GOTO :ErrorStop

@echo Build Debug ==============================
SET STEP=Debug

SET CONFIGURATION=%STEP%

SET STEP=%CONFIGURATION%

"%MSBUILDEXE%" /p:Configuration=%CONFIGURATION% %MSBUILDOPT% "%PROJ%"
@if ERRORLEVEL 1 GOTO :ErrorStop
@echo BUILD ok for %CONFIGURATION% %PROJ%

@echo Build Release ============================
SET STEP=Release

SET CONFIGURATION=%STEP%

"%MSBUILDEXE%" /p:Configuration=%CONFIGURATION% %MSBUILDOPT% "%PROJ%"
@if ERRORLEVEL 1 GOTO :ErrorStop
@echo BUILD ok for %CONFIGURATION% %PROJ%

if EXIST %PROJ_NAME%.nuspec (
  @echo ===== Build NuGet package for %PROJ% =====
  SET STEP=NuGet-Pack

  powershell -f %CMDHOME%\..\build\localmode\nugetpack.ps1
  @if ERRORLEVEL 1 GOTO :ErrorStop
  @echo NuGet package ok for %PROJ%
)

@echo ===== Build succeeded for %PROJ% =====

@GOTO :EOF

:ErrorStop
set RC=%ERRORLEVEL%
if "%STEP%" == "" set STEP=%CONFIGURATION%
@echo ===== Build FAILED for %PROJ% -- %STEP% with error %RC% - CANNOT CONTINUE =====
exit /B %RC%
:EOF
