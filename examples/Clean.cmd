@ECHO OFF

rem
rem Copyright (c) Microsoft. All rights reserved.
rem Licensed under the MIT license. See LICENSE file in the project root for full license information.
rem

FOR /D /R . %%G IN (bin) DO @IF EXIST "%%G" (@echo RDMR /S /Q "%%G" & rd /s /q "%%G")
FOR /D /R . %%G IN (obj) DO @IF EXIST "%%G" (@echo RDMR /S /Q "%%G" & rd /s /q "%%G")