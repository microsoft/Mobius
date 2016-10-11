@setlocal
@ECHO off

rem
rem Copyright (c) Microsoft. All rights reserved.
rem Licensed under the MIT license. See LICENSE file in the project root for full license information.
rem

SET CMDHOME=%~dp0
@REM Remove trailing backslash \
set CMDHOME=%CMDHOME:~0,-1%

if "%NUNITCONSOLE%" == "" (
  GOTO :NUNITCONSOLEERROR
)

cd "%CMDHOME%"
@cd

SET CONFIGURATION=Debug

set TESTS=AdapterTest\bin\%CONFIGURATION%\AdapterTest.dll

@Echo Test assemblies = %TESTS%

set TEST_ARGS=/framework:net-4.5

"%NUNITCONSOLE%" %TEST_ARGS% %TESTS%

if %ERRORLEVEL% NEQ 0 (
  set RC=%ERRORLEVEL%
  echo "==== Test failed ===="
  exit /B %RC%
)

echo "==== Test succeeded ==="
GOTO :EOF

:NUNITCONSOLEERROR
echo "[Test.cmd] Error - NUNITCONSOLE environment variable is not set"
echo "[Test.cmd] Please set NUNITCONSOLE to path\to\nunit\console"
exit /B 1
