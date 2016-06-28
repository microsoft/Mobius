# Building Mobius in Linux

## Requirements

* JDK 7 or above.
* Maven 3.0.5 or above.
* Mono 4.2 stable or above. The download and installation instructions for Mono are available in [http://www.mono-project.com/download/#download-lin](http://www.mono-project.com/download/#download-lin) (see [Debian, Ubuntu and derivatives](http://www.mono-project.com/docs/getting-started/install/linux/#debian-ubuntu-and-derivatives) or [CentOS, Fedora, similar Linux distributions or OS X](http://www.mono-project.com/docs/getting-started/install/linux/#centos-7-fedora-19-and-later-and-derivatives))
* F# for Mono. The download and installation instructions for the F# Mono extension are available in [http://fsharp.org/use/linux/](http://fsharp.org/use/linux/)
* NuGet.
* XSLTPROC

The following environment variables should be set properly:

* `JAVA_HOME`

## Instructions

Instructions to build Mobius in Linux are same as [instructions for Windows](windows-instructions.md#instructions). The only change required is to use the following script files instead of .cmd files:
* build.sh
* clean.sh

# Running Unit Tests in Linux

* Install NUnit Runner 3.0 or above using NuGet (see [https://www.nuget.org/packages/NUnit.Runners/](https://www.nuget.org/packages/NUnit.Runners/)), set `NUNITCONSOLE` to the path to nunit console, navigate to `Mobius/csharp` and run the following command:     
    ```
    ./test.sh
    ```
    
# Running Mobius Samples in Linux
Same as [instructions for Windows](windows-instructions.md#running-samples) but using the following scripts instead of .cmd files:
* run-samples.sh
* sparkclr-submit.sh

Note that paths to files and syntax of the environment variables (like $SPARKCLR_HOME) will need to be updated for Linux when following the instructions for Windows.

# Running Mobius Examples in Linux
Same as [instructions for Windows](./running-mobius-app.md#running-mobius-examples-in-local-mode) but with Linux scripts and paths. Refer to following general instructions for running any Mobius application in Linux.

# Running Mobius Applications in Linux

### Requirements
* Mono 4.2 stable or above. The download and installation instructions for Mono are available in [http://www.mono-project.com/download/#download-lin](http://www.mono-project.com/download/#download-lin) (see [Debian, Ubuntu and derivatives](http://www.mono-project.com/docs/getting-started/install/linux/#debian-ubuntu-and-derivatives) or [CentOS, Fedora, similar Linux distributions or OS X](http://www.mono-project.com/docs/getting-started/install/linux/#centos-7-fedora-19-and-later-and-derivatives)
* [Mobius release 1.6.101-PREVIEW1](https://github.com/Microsoft/Mobius/releases/tag/v1.6.101-PREVIEW-1) or above

## Instructions
The [instructions](./running-mobius-app.md#windows-instructions) for running Mobius applications in Windows are relevant for Linux as well. With the following tweaks, the same instructions can be used to run Mobius applications in Linux.
* Instead of `RunSamples.cmd`, use `run-samples.sh`
* Instead of `sparkclr-submit.cmd`, use `sparkclr-submit.sh`

If you are using CentOS, Fedora, or similar Linux distributions or OS X, follow the steps desicribed below that conforms to [Mono application depoyment guidelines](http://www.mono-project.com/docs/getting-started/application-deployment/)
  * Create a script (referred to as 'prefix script') that will use Mono to execute Mobius driver application. See the [linux-prefix-script.md](./linux-prefix-script.md) for a sample. The name of this script will be used in the place of the name of the mobius driver application when launching [sparkclr-submit.cmd](./linux-instructions.md#running-mobius-samples-in-linux)
  * Update CSharpWorkerPath setting in Mobius application config (refer to the config files used in Mobius examples like the [config for with Pi example](https://github.com/skaarthik/Mobius/blob/linux/examples/Batch/pi/App.config#L61)) to point to [CSharpWorker.sh.exe](./linux-csharpworker-prefix-script.md) (make sure to set the correct value appropriate for the Spark mode to be used)

**Note** - only client mode is support in Mobius on YARN in Linux. Support for [cluster mode](https://github.com/Microsoft/Mobius/issues/467) will be added soon.

### Mobius in Azure HDInsight Spark Cluster
* Mono version available in HDInsight cluster is 3.x. Mobius [requires](/notes/linux-instructions.md#prerequisites) 4.2 or above. So, Mono has to be upgraded in HDInsight cluster to use Mobius.
* Follow [instructions](./linux-instructions.md#requirements) for Ubuntu

### Mobius in Amazon Web Services EMR Spark Cluster
* Follow [instructions](./linux-instructions.md#requirements) for CentOS
* If there are issues with installing Mono following the previous step, consider doing the following to build and install Mono (instructions below are for version 4.4.1.0):

```bash
$ sudo yum -y install bison gettext glib2 freetype fontconfig libpng libpng-devel libX11 libX11-devel glib2-devel libexif glibc-devel urw-fonts java unzip gcc gcc-c++ automake autoconf libtool make bzip2 wget
$ cd /usr/local/src
$ sudo wget http://download.mono-project.com/sources/mono/mono-4.4.1.0.tar.bz2
$ sudo tar jxf mono-4.4.1.0.tar.bz2
$ cd mono-4.4.1.0
$ sudo ./configure --prefix=/opt/mono
$ sudo make 
$ sudo make install
```
