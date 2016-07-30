```bash
#!/bin/sh
# ## assuming mono in path /opt/mono below. Update the path if needed ##
export PATH=$PATH:/opt/mono/bin
export PKG_CONFIG_PATH=/opt/mono/lib/pkgconfig
# ## uncomment one of the following lines depending on the mode of execution
# ## for Spark in YARN mode ##
#exec mono ./CSharpWorker.exe "$@"
# ## for Spark in local mode ##
# ## set the correct path to your application below ##
#exec mono /path/to/your/mobius/application/CSharpWorker.exe "$@"
```
