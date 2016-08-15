# Troubleshoot Errors in Mobius Apps

|Error |Error Details | Possible Fix |
|----|------|------|
|Static method collectAndServe failed for class org.apache.spark.api.python.PythonRDD |Exception java.net.SocketException: Connection reset by peer: socket write error at java.net.SocketOutputStream.socketWrite0 | Try explicitly setting spark.local.dir command line parameter to some location like C:\Temp\SparkCLRTemp. If the default temp location (C:\Users\userName\AppData\Local\Temp) is used, Windows Defender might delete CSharpWorker.exe (thinking it is a malicious exe) when it gets downloaded by Spark into this temp location.|
|BadImageFormatException | Unhandled Exception: System.BadImageFormatException: An Attemtpt was made to load a program with an incorrect format. | This error could happen when using "Rio" socket type and "Prefer 32-bit" option is set to true when building the Mobius driver program. The fix is to set this opton to "false" or using some other socket type ("Normal" or "Saea") that does not depend on a unmanaged component RioSock.dll that is built for 64-bit configuration.|
