@ECHO OFF
FOR /D /R . %%G IN (bin) DO @IF EXIST "%%G" (@echo RDMR /S /Q "%%G" & rd /s /q "%%G")
FOR /D /R . %%G IN (obj) DO @IF EXIST "%%G" (@echo RDMR /S /Q "%%G" & rd /s /q "%%G")
FOR /D /R . %%G IN (x64) DO @IF EXIST "%%G" (@echo RDMR /S /Q "%%G" & rd /s /q "%%G")