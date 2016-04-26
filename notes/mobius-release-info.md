# GitHub Release
The [release in GitHub](https://github.com/Microsoft/Mobius/releases) is a zip file. When you unzip that file, you will see a directory layout as follows:

````    
|-- examples
    |-- Example Mobius applications
|-- localmode
    |-- Scripts for running samples and examples in local mode
|-- runtime
    |-- bin
        |-- .NET binaries and its dependencies used by Mobius applications
    |-- data    
        |-- Data files used by the [samples](..\csharp\Samples\Microsoft.Spark.CSharp)
    |-- examples    
        |-- C# Spark driver [examples](..\examples) implemented using Mobius        
    |-- lib
        |-- Mobius jar file
    |-- samples
        |-- C# Spark driver [samples](..\csharp\Samples\Microsoft.Spark.CSharp) for Moibus API  
    |-- scripts
        |-- Mobius job submission scripts
```` 

You can run all the samples locally by invoking `localmode\RunSamples.cmd`. The script automatically downloads Apache Spark distribution and run the samples on your local machine. Note: Apache Spark distribution is a greater than 200 Mbytes download; `Runsamples.cmd` only downloads the Apache Spark distribution once.
[Mobius examples](..\examples) may have external dependencies and may need configuration settings to those dependencies before they can be run.

# NuGet Package
The packages published to [NuGet](https://www.nuget.org/packages/Microsoft.SparkCLR/) are primarily for references when building Mobius application. If Visual Studio is used for development. the reference to the NuGet package will go in packages.config file.

# Versioning Policy
|Release location|Naming convention |Example |
|----|----|----|
|[GitHub](https://github.com/Microsoft/Mobius/releases) |spark-clr_[scala version]-[spark version][mobius release id][optional suffix].zip|spark-clr_2.10-1.6.100-PREVIEW-1.zip |
|[NuGet](https://www.nuget.org/packages/Microsoft.SparkCLR/)|Microsoft.SparkCLR [spark version][mobius release id][optional suffix]|Microsoft.SparkCLR 1.6.100-PREVIEW-1 |

* **[scala version]** - version of scala used to build Spark (like 2.10 or 2.11)
* **[spark version]** - version of Spark (like 1.5.2 or 1.6.1)
* **[mobius release id]** - identifier for Mobius release (like 00, 01 etc.)
* **[optional suffix]** - used for indicating pre-releases (like PREVIEW-1, PREVIEW-2 etc.)
