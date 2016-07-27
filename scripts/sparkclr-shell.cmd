@echo off

rem
rem Copyright (c) Microsoft. All rights reserved.
rem Licensed under the MIT license. See LICENSE file in the project root for full license information.
rem

rem This is the entry point for running Mobius shell. To avoid polluting the
rem environment, it just launches a new cmd to do the real work.

cmd /V /E /C %~dp0sparkclr-submit.cmd --name MobiusShell %* --exe Repl.exe %~dp0..\repl