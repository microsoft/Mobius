#
# Input - 
#     (1) "stage" parameter, accepts either "build" or "run"
#     (2) "vebose" parameter, accepts "verbose"
#
Param([string] $stage, [string] $verbose)

if ($stage.ToLower() -eq "run")
{
    # retrieve hadoop and spark versions from environment variables
    $envValue = [Environment]::GetEnvironmentVariable("HADOOP_VERSION")
    $hadoopVersion = if ($envValue -eq $null) { "2.6" } else { $envValue }
    
    $envValue = [Environment]::GetEnvironmentVariable("SPARK_VERSION")
    $sparkVersion = if ($envValue -eq $null) { "1.6.0" } else { $envValue }
    
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

#
# Input: 
#   (1) $variable to be replaced; 
#   (2) $value to fill in; 
#   (3) $sourceFile; 
#   (4) $targetFile with $variable replaced by $value
#
function Replace-VariableInFile($variable, $value, $sourceFile, $targetFile)
{
    Write-Output "[downloadtools.Replace-VariableInFile] variable=$variable, value=$value, sourceFile=$sourceFile, targetFile=$targetFile"
    if (!(test-path $sourceFile))
    {
        Write-Output "[downloadtools.Replace-VariableInFile] [WARNING] $sourceFile does not exist. Abort."
        return
    }

    if ([string]::IsNullOrEmpty($variable))
    {
        Write-Output "[downloadtools.Replace-VariableInFile] [WARNING] variable name is empty. Abort."
        return
    }

    $now = Get-Date
    Write-Host "[Replace-VariableInFile] [$now] replace $variable in $sourceFile to produce $targetFile"
    (get-content $sourceFile) | Foreach-Object {
        $_ -replace "$variable", $value `
        } | Set-Content $targetFile -force
}

function Download-File($url, $output)
{
    if (test-path $output)
    {
        Write-Output "[downloadtools.Download-File] $output exists. No need to download."
        return
    }

    $start_time = Get-Date
    $wc = New-Object System.Net.WebClient
    Write-Output "[downloadtools.Download-File] Start downloading $url to $output ..."
    $Global:downloadComplete = $false
    Register-ObjectEvent -InputObject $wc -EventName DownloadFileCompleted `
        -SourceIdentifier Web.DownloadFileCompleted -Action {
        $Global:downloadComplete = $True
    }
    Register-ObjectEvent -InputObject $wc  -EventName DownloadProgressChanged `
        -SourceIdentifier Web.DownloadProgressChanged -Action {
        $Global:Data = $event
    }
    $wc.DownloadFileAsync($url, $output)
    While (!($Global:downloadComplete)) {
        $percent = $Global:Data.SourceArgs.ProgressPercentage
        $totalBytes = $Global:Data.SourceArgs.TotalBytesToReceive
        $receivedBytes = $Global:Data.SourceArgs.BytesReceived
        If ($percent -ne $null) {
            Write-Progress -Activity ("Downloading file to {0} from {1}" -f $output,$url) -Status ("{0} bytes \ {1} bytes" -f $receivedBytes,$totalBytes)  -PercentComplete $percent
        }
    }
    Write-Progress -Activity ("Downloading file to {0} from {1}" -f $output, $url) -Status ("{0} bytes \ {1} bytes" -f $receivedBytes,$totalBytes)  -Completed
    Unregister-Event -SourceIdentifier Web.DownloadFileCompleted
    Unregister-Event -SourceIdentifier Web.DownloadProgressChanged
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

    Write-Output "[downloadtools.Download-File] Download completed. Time taken: $howlong"
}

function Unzip-File($zipFile, $targetDir)
{
    if (!(test-path $zipFile))
    {
        Write-Output "[downloadtools.Unzip-File] WARNING!!! $zipFile does not exist. Abort."
        return
    }

    if (!(test-path $targetDir))
    {
        Write-Output "[downloadtools.Unzip-File] $targetDir does not exist. Creating ..."
        New-Item -ItemType Directory -Force -Path $targetDir | Out-Null
        Write-Output "[downloadtools.Unzip-File] Created $targetDir."
    }

    $start_time = Get-Date
    Write-Output "[downloadtools.Unzip-File] Extracting $zipFile to $targetDir ..."
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

    Write-Output "[downloadtools.Unzip-File] Extraction completed. Time taken: $howlong"
}

function Untar-File($tarFile, $targetDir)
{
    if (!(test-path $tarFile))
    {
        Write-Output "[downloadtools.Untar-File] WARNING!!! $tarFile does not exist. Abort."
        return
    }

    if (!(test-path $targetDir))
    {
        Write-Output "[downloadtools.Untar-File] $targetDir does not exist. Creating ..."
        New-Item -ItemType Directory -Force -Path $targetDir | Out-Null
        Write-Output "[downloadtools.Untar-File] Created $targetDir."
    }

    $start_time = Get-Date

    Write-Output "[downloadtools.Untar-File] Extracting $tarFile to $targetDir ..."
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

    Write-Output "[downloadtools.Untar-File] Extraction completed. Time taken: $howlong"
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
        Write-Output "[downloadtools.Download-BuildTools] $tarToolExe exists already. No download and extraction needed"
    }
    
    if (!($path -like "*$toolsDir*"))
    {
        # add toolsdir to path
        $envStream.WriteLine("set path=$toolsDir;%path%");
    }
    
    # Apache Maven
	$mvnVer = "apache-maven-3.3.3"
    $mvnCmd = "$toolsDir\$mvnVer\bin\mvn.cmd"
    if (!(test-path $mvnCmd))
    {
        $url = "http://www.us.apache.org/dist/maven/maven-3/3.3.3/binaries/$mvnVer-bin.tar.gz"
        $output="$toolsDir\$mvnVer-bin.tar.gz"
        Download-File $url $output
        Untar-File $output $toolsDir

        # Add downloaded Mvn to path + env
        $envStream.WriteLine("set M2_HOME=$toolsDir\$mvnVer");
        $envStream.WriteLine("set M2=%M2_HOME%\bin");
    }
    else
    {
        Write-Output "[downloadtools.Download-BuildTools] $mvnCmd exists already. No download and extraction needed"
    }
    
    $mavenBin = "$toolsDir\$mvnVer\bin"
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
        Write-Output "[downloadtools.Download-BuildTools] $nugetExe exists already. No download and extraction needed"
    }

    # gpg4win
    if ($env:APPVEYOR_REPO_TAG -eq "true")
    {
        $gpgZip = "$toolsDir\gpg4win-vanilla-2.3.0.zip"
        if (!(test-path $gpgZip))
        {
            $url = "https://github.com/SparkCLR/build/blob/master/tools/gpg4win-vanilla-2.3.0.zip?raw=true"
            $output=$gpgZip
            Download-File $url $output
            # Unzip-File $output $toolsDir
            Write-Output "[downloadtools.Download-BuildTools] Extracting $output to $toolsDir ..."
            Invoke-Expression "& 7z x $output -o$toolsDir"
        }
        else
        {
            Write-Output "[downloadtools.Download-BuildTools] $gpgZip exists already. No download and extraction needed"
        }

    	$gpgBin = "$toolsDir\GnuPG\pub"
    	if (!($path -like "*$gpgBin*"))
    	{
            # add maven bin
            $envStream.WriteLine("set path=$gpgBin\;%path%");
    	}
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
        Write-Output "[downloadtools.Download-BuildTools] $tarToolExe exists already. No download and extraction needed"
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
        Write-Output "[downloadtools.Download-RuntimeDependencies] $sparkSubmit exists already. No download and extraction needed"
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
        Write-Output "[downloadtools.Download-RuntimeDependencies] $winutilsExe exists already. No download and extraction needed"
    }

    $envStream.WriteLine("set HADOOP_HOME=$H_HOME");

    $envStream.close()

    Update-SparkVerboseMode
    Update-CSharpVerboseMode

    return
}

function Update-SparkVerboseMode
{
    $temp = [Environment]::GetEnvironmentVariable("temp").ToLower()
    if ($verbose -ne "verbose")
    {
        #
        # Out of the box, Spark logs to console. 
        # Customized log4j.properites under spark.conf replaces consoler appender with a rolling file appender,
        # which creates logs under {env:TEMP} directory.
        # the script below replaces ${env:TEMP} with actual %TEMP% path
        #
        
        # convert temp path to unix-style path
        $tempValue = $temp -replace "\\", "/"
    
        # replace {env:TEMP} with temp path
        $targetFile = "$temp\log4j.properties.temp"
        Replace-VariableInFile '\${env:TEMP}' "$tempValue" "$scriptDir\spark.conf\log4j.properties" $targetFile
    
        # copy customized log4j properties to SPARK_HOME\conf
        copy-item  $scriptDir\spark.conf\*.properties $S_HOME\conf -force
        copy-item  $targetFile $S_HOME\conf\log4j.properties -force
    }
    else
    {
        #
        # remove customized log4j.properties, revert back to out-of-the-box Spark logging to console
        #
        
        $propertyFiles = get-childitem $S_HOME\conf -filter *.properties
        if ($propertyFiles.Count -gt 0)
        {
            remove-item $S_HOME\conf\*.properties -force
        }
    }

    return
}

function Backup-CSharpConfig($configPath, $originalSuffix)
{
    $configFiles = get-childitem $configPath -filter *.config

    pushd $configPath
    foreach ($file in $configFiles)
    {
        $name = $file.Name
        $original = "$name$originalSuffix"
        if (! (Test-Path($original)))
        {
            copy-item $name $original -force
        }
    }
    popd
}

function Update-CSharpVerboseMode
{
    $configPath = "$scriptDir\..\runtime\samples"
    $originalSuffix = ".orginal"
    Backup-CSharpConfig $configPath $originalSuffix

    if ($verbose -ne "verbose")
    {
        #
        # Disable (comment out) console appender in worker and sample.config files
        #
        $configPath = "$scriptDir\..\runtime\samples"
        $configFiles = get-childitem $configPath -filter *.config

        pushd $configPath
        foreach ($file in $configFiles)
        {
            $name = $file.Name
            $original = "$name$originalSuffix"
            if (Test-Path($original))
            {
                Replace-VariableInFile '<appender-ref\s*ref="ConsoleAppender"\s*/>' '<!--<appender-ref ref="ConsoleAppender" />-->' $original $name
                Write-Output "[downloadtools.Update-VerboseMode] enabled console appender in $name, under $configPath"
            }
            else
            {
                Write-Output "[downloadtools.Update-VerboseMode] [Warning] missing $original under $configPath"
            }
        }
        popd
    }
    else
    {
        #
        # Restore worker and sample.config files to the original versions that do have console appender
        #
        $configFiles = get-childitem $configPath -filter *.config
        pushd $configPath
        foreach ($file in $configFiles)
        {
            $name = $file.Name
            $original = "$name$originalSuffix"
            if (Test-Path($original))
            {
                # original config file supports non-verbose mode, without consoler appender
                copy-item $original $name -force
                Write-Output "[downloadtools.Update-VerboseMode] $name restored from $original, under $configPath"
            }
            else
            {
                Write-Output "[downloadtools.Update-VerboseMode] [Warning] missing $original under $configPath"
            }
        }
        popd
    }

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

# Create tools directory
$scriptDir = Get-ScriptDirectory
$toolsDir = "$scriptDir\..\tools"
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
