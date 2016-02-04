pushd %~dp0

set MICROSOFT_NET_COMPILERS_VERSION=1.1.1

sparkclr-submit.cmd --conf spark.local.dir=..\Temp --name SparkCLRShell %* --exe shell.exe ..\shell

popd