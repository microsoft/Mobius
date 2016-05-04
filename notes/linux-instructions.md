# Building Mobius

## Prerequisites

* JDK 7 or above.
* Maven 3.3.3 or above.
* Mono 4.2 stable or above. The download and installation instructions for Mono are available in [http://www.mono-project.com/download/#download-lin](http://www.mono-project.com/download/#download-lin).
* NuGet.
* XSLTPROC

The following environment variables should be set properly:

* `JAVA_HOME`

## Instructions
Same as [instructions for Windows](windows-instructions.md#instructions) but use the following script files instead of .cmd files:
* build.sh
* clean.sh

# Running Unit Tests

* Install NUnit Runner 3.0 or above using NuGet (see [https://www.nuget.org/packages/NUnit.Runners/](https://www.nuget.org/packages/NUnit.Runners/)), set `NUNITCONSOLE` to the path to nunit console, navigate to `Mobius/csharp` and run the following command:     
    ```
    ./test.sh
    ```

# Running Samples
Same as [instructions for Windows](windows-instructions.md#running-samples) but using the following scripts instead of .cmd files:
* run-samples.sh
* sparkclr-submit.sh

Note that paths to files and syntax of the environment variables (like $SPARKCLR_HOME) will need to be updated for Linux when following the instructions for Windows.
