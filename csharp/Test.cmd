@setlocal
@ECHO off

SET CMDHOME=%~dp0
@REM Remove trailing backslash \
set CMDHOME=%CMDHOME:~0,-1%

SET VSIDEDIR=%VS120COMNTOOLS%..\IDE
SET VSTESTEXEDIR=%VSIDEDIR%\CommonExtensions\Microsoft\TestWindow
SET VSTESTEXE=%VSTESTEXEDIR%\VSTest.console.exe

cd "%CMDHOME%"
@cd

SET CONFIGURATION=Release

set TESTS=AdapterTest\bin\%CONFIGURATION%\AdapterTest.dll

@Echo Test assemblies = %TESTS%

set TEST_ARGS= /Framework:Framework45

"%VSTESTEXE%" %TEST_ARGS% %TESTS%
