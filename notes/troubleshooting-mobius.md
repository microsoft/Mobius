# Troubleshoot Errors in Mobius Apps

|Error |Error Details | Possible Fix |
|----|------|------|
|Static method collectAndServe failed for class org.apache.spark.api.python.PythonRDD |Exception java.net.SocketException: Connection reset by peer: socket write error at java.net.SocketOutputStream.socketWrite0 | Try explicitly setting spark.local.dir command line parameter to some location like C:\Temp\SparkCLRTemp. If the default temp location (C:\Users\userName\AppData\Local\Temp) is used, Windows Defender might delete CSharpWorker.exe (thinking it is a malicious exe) when it gets downloaded by Spark into this temp location.|
