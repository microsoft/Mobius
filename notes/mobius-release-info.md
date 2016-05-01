# GitHub Release
The [release in GitHub](https://github.com/Microsoft/Mobius/releases) is a zip file. When you unzip that file, you will see a directory layout as follows:

````    
|-- mobius-release-info.md
|-- runtime
    |-- bin
        |-- .NET binaries and its dependencies used by Mobius applications
    |-- dependencies    
        |-- jar files Mobius depends on for functionality like CSV parsing, Kafka message processing etc.        
    |-- lib
        |-- Mobius jar file
    |-- scripts
        |-- Mobius job submission scripts
|-- examples
    |-- Example Mobius applications
|-- samples
    |-- C# Spark driver samples for Mobius API 
    |-- data    
        |-- Data files used by the samples
```` 

Instructions on running a Mobius app is available at https://github.com/Microsoft/Mobius/blob/master/notes/running-mobius-app.md

Mobius samples do not have any extenral dependencies. The dependent jar files and data files used by samples are included in the release. Instructions to run samples are available at
* https://github.com/Microsoft/Mobius/blob/master/notes/windows-instructions.md#running-samples for Windows
* https://github.com/Microsoft/Mobius/blob/master/notes/linx-instructions.md#running-samples for Linux

Mobius examples under "examples" folder may have external dependencies and may need configuration settings to those dependencies before they can be run. Refer to [Running Examples](https://github.com/Microsoft/Mobius/blob/master/notes/running-mobius-app.md#running-mobius-examples-in-local-mode) for details on how to run each example.

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
