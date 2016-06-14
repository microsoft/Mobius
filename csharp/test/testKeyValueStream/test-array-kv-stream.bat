@echo off
setlocal enabledelayedexpansion

set shellDir=%~dp0
set exeDir=%shellDir%bin\Debug

if "%1" == "" (
    %exeDir%\testKeyValueStream.exe
    exit /b 0
)

pushd %CD%

pushd %~dp0..\..\..
set codeRoot=%CD%

set HADOOP_HOME=%codeRoot%\build\tools\winutils
set SPARK_HOME=%codeRoot%\build\tools\spark-1.6.1-bin-hadoop2.6
set SPARKCLR_HOME=%codeRoot%\build\runtime

pushd %~dp0\bin\Debug 

set options=--executor-cores 2 --driver-cores 2 --executor-memory 1g --driver-memory 1g

echo %SPARKCLR_HOME%\scripts\sparkclr-submit.cmd --verbose %options% --exe testKeyValueStream.exe %CD% %*
%SPARKCLR_HOME%\scripts\sparkclr-submit.cmd --verbose %options% --exe testKeyValueStream.exe %CD% %*

popd

popd

@exit /b 0

::=== following are test commands example ==========================================

:: Enable/Disable echo in *.cmd files for debug : use -R to replace ; Without -R to preview
lzmw -f "\.cmd$" -d "tools|scripts|localmode" -it "^(\s*@?\s*echo\s+)off" -o "$1 on" -rp d:\msgit\lqmMobius  -R
lzmw -f "\.cmd$" -d "tools|scripts|localmode" -it "^(\s*@?\s*echo\s+)on" -o "$1 off" -rp d:\msgit\lqmMobius  -R

:: Start source stream socket and test
d:\msgit\lqmMobius\csharp\test\SourceLinesSocket\bin\Debug\SourceLinesSocket.exe 9111 100 0
d:\msgit\lqmMobius\csharp\test\testKeyValueStream\test-array-kv-stream.bat 127.0.0.1 9111 1 30 20 checkDir 0 1 0 10485760  2>d:\tmp\logKvError.log > d:\tmp\logKvOut.log

:: Find log in directory with above logs in directory d:\tmp
lzmw -p d:\tmp -f "^logKv.*\.log$" -it "cannot|fail|error|exception" --nt "sleep interrupted|goto|echo\s+" -U 9 -D 9 -c
lzmw -p d:\tmp -f logKv -it "thread st\w+|released [1-9]\d*|alive objects|used \d+|(begin|end) of|dispose"

:: Find latest log ignore file name under Cygwin on Windows in directory /cygdrive/d/tmp/ 
lzmw -c --w1 "$(lzmw -l --wt -T 2 -PIC 2>/dev/null | head -n 1 | awk -F '\t' '{print $1}' )" -it "cannot|fail|error|exception" --nt "sleep interrupted|goto|echo\s+" -U 9 -D 9
lzmw -c --w1 "$(lzmw -l --wt -T 2 -PIC 2>/dev/null | head -n 1 | awk -F '\t' '{print $1}' )" -it "thread st\w+|dispose|released [1-9]\d*|finished all" -e "\d+ alive"
lzmw -c --w1 "$(lzmw -l --wt -T 2 -PIC 2>/dev/null | head -n 1 | awk -F '\t' '{print $1}' )" -it "JVMObjectTracker" -H 3 -T 3
lzmw -c --w1 "$(lzmw -l --wt -T 2 -PIC 2>/dev/null | head -n 1 | awk -F '\t' '{print $1}' )" -it "thread st\w+|released [1-9]\d*|alive objects|used \d+|(begin|end) of|dispose"

:: Kill test process under Cygwin on Windows
for pid in $(wmic process get processid, name | lzmw -it '^.*testKeyValueStream.exe\s+(\d+).*$' -o '$1' -PAC); do taskkill /f /pid $pid ; done
