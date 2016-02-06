FOR /F "tokens=*" %%G IN ('DIR /B /AD /S bin') DO RMDIR /S /Q "%%G"
FOR /F "tokens=*" %%G IN ('DIR /B /AD /S obj') DO RMDIR /S /Q "%%G"
@REM FOR /F "tokens=*" %%G IN ('DIR /B /AD /S TestResults') DO RMDIR /S /Q "%%G"