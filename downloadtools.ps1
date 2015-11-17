#
# This script takes in "stage" parameter, which should be either "build" or "run"
#
Param([string] $stage)

if ($stage.ToLower() -eq "run")
{
    # retrieve hadoop and spark versions from environment variables
    $envValue = [Environment]::GetEnvironmentVariable("HADOOP_VERSION")
    $hadoopVersion = if ($envValue -eq $null) { "2.6" } else { $envValue }
    
    $envValue = [Environment]::GetEnvironmentVariable("SPARK_VERSION")
    $sparkVersion = if ($envValue -eq $null) { "1.4.1" } else { $envValue }
    
    Write-Output "[downloadtools] hadoopVersion=$hadoopVersion, sparkVersion=$sparkVersion"
}

function Get-ScriptDirectory
{
    $Invocation = (Get-Variable MyInvocation -Scope 1).Value;
    if($Invocation.PSScriptRoot)
    {
        $Invocation.PSScriptRoot;
    }
    Elseif($Invocation.MyCommand.Path)
    {
        Split-Path $Invocation.MyCommand.Path
    }
    else
    {
        $Invocation.InvocationName.Substring(0,$Invocation.InvocationName.LastIndexOf("\"));
    }
}

function Download-File($url, $output)
{
    if ((test-path $output))
    {
        Write-Output "[Download-File] $output exists. No need to download."
        return
    }

    $start_time = Get-Date
    $wc = New-Object System.Net.WebClient
    Write-Output "[Download-File] Start downloading $url to $output ..."
    $wc.DownloadFile($url, $output)
    $duration = $(Get-Date).Subtract($start_time)
    if ($duration.Seconds -lt 2)
    {
        $mills = $duration.MilliSeconds
        $howlong = "$mills milliseconds"
    }
    else
    {
        $seconds = $duration.Seconds
        $howlong = "$seconds seconds"
    }

    Write-Output "[Download-File] Download completed. Time taken: $howlong"
}

function Unzip-File($zipFile, $targetDir)
{
    if (!(test-path $zipFile))
    {
        Write-Output "[Unzip-File] WARNING!!! $zipFile does not exist. Abort."
        return
    }

    if (!(test-path $targetDir))
    {
        Write-Output "[Unzip-File] $targetDir does not exist. Creating ..."
        New-Item -ItemType Directory -Force -Path $targetDir | Out-Null
        Write-Output "[Unzip-File] Created $targetDir."
    }

    $start_time = Get-Date
    Write-Output "[Unzip-File] Extracting $zipFile to $targetDir ..."
    $entries = [IO.Compression.ZipFile]::OpenRead($zipFile).Entries
    $entries | 
        %{
            #compose some target path
            $targetpath = join-path "$targetDir" $_.FullName
            #extract the file (and overwrite)
            [IO.Compression.ZipFileExtensions]::ExtractToFile($_, $targetpath, $true)
        }
    
    $duration = $(Get-Date).Subtract($start_time)
    if ($duration.Seconds -lt 2)
    {
        $mills = $duration.MilliSeconds
        $howlong = "$mills milliseconds"
    }
    else
    {
        $seconds = $duration.Seconds
        $howlong = "$seconds seconds"
    }

    Write-Output "[Unzip-File] Extraction completed. Time taken: $howlong"
}

function Untar-File($tarFile, $targetDir)
{
    if (!(test-path $tarFile))
    {
        Write-Output "[Untar-File] WARNING!!! $tarFile does not exist. Abort."
        return
    }

    if (!(test-path $targetDir))
    {
        Write-Output "[Untar-File] $targetDir does not exist. Creating ..."
        New-Item -ItemType Directory -Force -Path $targetDir | Out-Null
        Write-Output "[Untar-File] Created $targetDir."
    }

    $start_time = Get-Date

    Write-Output "[Untar-File] Extracting $tarFile to $targetDir ..."
    Invoke-Expression "& `"$tarToolExe`" $tarFile $targetDir"
    
    $duration = $(Get-Date).Subtract($start_time)
    if ($duration.Seconds -lt 2)
    {
        $mills = $duration.MilliSeconds
        $howlong = "$mills milliseconds"
    }
    else
    {
        $seconds = $duration.Seconds
        $howlong = "$seconds seconds"
    }

    Write-Output "[Untar-File] Extraction completed. Time taken: $howlong"
}

function Download-BuildTools
{
    # Create a cmd file to update environment variable
    $path = [Environment]::GetEnvironmentVariable("path").ToLower()
    $envStream = [System.IO.StreamWriter] "$toolsDir\updatebuildtoolenv.cmd"
    
    # TarTool
    $tarToolExe = "$toolsDir\TarTool.exe"
    if (!(test-path $tarToolExe))
    {
        $url = "http://download-codeplex.sec.s-msft.com/Download/Release?ProjectName=tartool&DownloadId=79064&FileTime=128946542158770000&Build=21031"
        $output="$toolsDir\TarTool.zip"
        Download-File $url $output
        Unzip-File $output $toolsDir
    }
    else
    {
        Write-Output "[Download-BuildTools] $tarToolExe exists already. No download and extraction needed"
    }
    
    if (!($path -like "*$toolsDir*"))
    {
        # add toolsdir to path
        $envStream.WriteLine("set path=$toolsDir;%path%");
    }
    
    # Apache Maven
    $mvnCmd = "$toolsDir\apache-maven-3.3.3\bin\mvn.cmd"
    if (!(test-path $mvnCmd))
    {
        $url = "http://www.us.apache.org/dist/maven/maven-3/3.3.3/binaries/apache-maven-3.3.3-bin.tar.gz"
        $output="$toolsDir\apache-maven-3.3.3-bin.tar.gz"
        Download-File $url $output
        Untar-File $output $toolsDir
    }
    else
    {
        Write-Output "[Download-BuildTools] $mvnCmd exists already. No download and extraction needed"
    }
    
    $mavenBin = "$toolsDir\apache-maven-3.3.3\bin"
    if (!($path -like "*$mavenBin*"))
    {
        # add maven bin
        $envStream.WriteLine("set path=$mavenBin\;%path%");
    }
    
    # Nuget Client
    $nugetExe = "$toolsDir\nuget.exe"
    if (!(test-path $nugetExe))
    {
        $url = "http://dist.nuget.org/win-x86-commandline/latest/nuget.exe"
        $output=$nugetExe
        Download-File $url $output
    }
    else
    {
        Write-Output "[Download-BuildTools] $nugetExe exists already. No download and extraction needed"
    }

    $envStream.close()
}

function Download-RuntimeDependencies
{
    # Create a cmd file to update environment variable
    $path = [Environment]::GetEnvironmentVariable("path").ToLower()
    $envStream = [System.IO.StreamWriter] "$toolsDir\updateruntime.cmd"

    # TarTool
    $tarToolExe = "$toolsDir\TarTool.exe"
    if (!(test-path $tarToolExe))
    {
        $url = "http://download-codeplex.sec.s-msft.com/Download/Release?ProjectName=tartool&DownloadId=79064&FileTime=128946542158770000&Build=21031"
        $output="$toolsDir\TarTool.zip"
        Download-File $url $output
        Unzip-File $output $toolsDir
    }
    else
    {
        Write-Output "[Download-BuildTools] $tarToolExe exists already. No download and extraction needed"
    }
    
    if (!($path -like "*$toolsDir*"))
    {
        # add toolsdir to path
        $envStream.WriteLine("set path=$toolsDir;%path%");
    }

    # Download Spark binary
    $S_HOME = "$toolsDir\spark-$sparkVersion-bin-hadoop$hadoopVersion"
    $sparkSubmit="$S_HOME\bin\spark-submit.cmd"
    if (!(test-path $sparkSubmit))
    {
        $url = "http://www.us.apache.org/dist/spark/spark-$sparkVersion/spark-$sparkVersion-bin-hadoop$hadoopVersion.tgz"
        $output = "$toolsDir\spark-$sparkVersion-bin-hadoop$hadoopVersion.tgz"
        Download-File $url $output
        Untar-File $output $toolsDir
    }
    else
    {
        Write-Output "[Download-RuntimeDependencies] $sparkSubmit exists already. No download and extraction needed"
    }

    $envStream.WriteLine("set SPARK_HOME=$S_HOME");

    if (!($path -like "*$S_HOME\bin*"))
    {
        # add spark_home\bin to path
        $envStream.WriteLine("set path=%path%;$S_HOME\bin");
    }

    # Download winutils.exe
    $H_HOME = "$toolsDir\winutils"
    $winutilsBin = "$H_HOME\bin"
    if (!(test-path "$winutilsBin"))
    {
        New-Item -ItemType Directory -Force -Path $winutilsBin | Out-Null
    }

    $winutilsExe = "$winutilsBin\winutils.exe"
    if (!(test-path $winutilsExe))
    {
        $url = "http://public-repo-1.hortonworks.com/hdp-win-alpha/winutils.exe"
        $output=$winutilsExe
        Download-File $url $output
    }
    else
    {
        Write-Output "[Download-RuntimeDependencies] $winutilsExe exists already. No download and extraction needed"
    }

    $envStream.WriteLine("set HADOOP_HOME=$H_HOME");

    $envStream.close()

    return
}

function Print-Usage
{
    Write-Output '====================================================================================================='
    Write-Output ''
    Write-Output '    This script takes one input parameter ("stage"), which can be either [build | run].'
    Write-Output ''
    Write-Output '        Build: Download tools required in building SparkCLR;'
    Write-Output '        Run: Download Apache Spark and related binaries, required to run SparkCLR samples locally.'
    Write-Output ''
    Write-Output '====================================================================================================='
}

#
# main body of the script
#

if (!($PSBoundParameters.ContainsKey('stage')))
{
    Print-Usage
    return
}

# Creat tools directory
$scriptDir = Get-ScriptDirectory
$toolsDir = Join-Path -path $scriptDir -ChildPath tools
New-Item -ItemType Directory -Force -Path $toolsDir | Out-Null
pushd "$toolsDir"
    
# Load IO.Compression.FileSystem for unzip capabilities
Add-Type -assembly "system.io.compression.filesystem"

if ($stage.ToLower() -eq "build")
{
    Download-BuildTools
}
elseif ($stage.ToLower() -eq "run")
{
    Download-RuntimeDependencies
}
else
{
    Print-Usage
}

popd

