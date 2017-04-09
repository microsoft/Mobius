@echo off
@rem Windows 7/8/10 may not allow powershell scripts by default
powershell -Command Set-ExecutionPolicy -Scope CurrentUser Unrestricted

@rem Setup init environment
pushd "%CMDHOME%"
powershell -f Init.ps1 -SetupEnv
popd
