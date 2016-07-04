```bash
#!/bin/sh
# ## assuming mono in path /opt/mono below. Update the path if needed ##
export PATH=$PATH:/opt/mono/bin
export PKG_CONFIG_PATH=/opt/mono/lib/pkgconfig
exec mono /path/to/your/mobius/application/SparkClrPi.exe "$@"
```
