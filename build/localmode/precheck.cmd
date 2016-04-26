@echo OFF

set precheck=ok

if not exist "%JAVA_HOME%\bin\java.exe" (
    @echo. 
    @echo ============================================================================================
    @echo. 
    @echo WARNING!!! %~nx0 detected JAVA_HOME is not set properly. Mobius requires JDK 7u85 and above, 
    @echo            or JDK 8u60 and above. You can either download OpenJDK available at 
    @echo            http://www.azul.com/downloads/zulu/zulu-windows/, or use Oracle JDK. 
    @echo. 
    @echo ============================================================================================
    @echo. 
    set precheck=bad
    goto :eof
)

set path=%path%;%JAVA_HOME%\bin

set version=unknown
@echo VisualStudioVersion = "%VisualStudioVersion%"
if "%VisualStudioVersion%" == "" ( goto vstudiowarning)

@REM VS 2013 == Version 12; VS 2015 == Version 14
set version=%VisualStudioVersion:~0,2%
if %version% LSS 12 ( goto vstudiowarning)

goto :eof

:vstudiowarning
@echo. 
@echo ============================================================================================
@echo. 
@echo WARNING!!! %~nx0 detected version of Visual Studio in current command prompt as %version%. 
@echo            Mobius %~nx0 requires "Developer Command Prompt for VS2013" and above, or 
@echo            "MSBuild Command Prompt for VS2015" and above.
@echo. 
@echo ============================================================================================
@echo. 
@echo Environment Variables
@echo. 
set
