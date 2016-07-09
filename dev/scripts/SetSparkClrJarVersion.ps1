#
# This script takes in "version" and "targetDir" (optional) parameters, update Spark-Clr jar 
# version reference in all scripts under "targetDir". 
#
# "targetDir" parameter is default to current directory where this script is located, when not provided.
#
Param([string]$targetDir, [string]$version)

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

function Update-SparkClrSubmit($targetDir, $version)
{
    if (!(test-path $targetDir))
    {
        Write-Output "[SetSparkClrJarVersion.Update-SparkClrSubmit] WARNING!!! $targetDir does not exist. Please provide a valid directory name !"
        return
    }

    Write-Output "[SetSparkClrJarVersion.Update-SparkClrSubmit] Start setting sparkclr-submit.cmd under $targetDir to version=$version"

    # 
    #  Update Mobius package version to this release. The version string in sparkclr-submit.cmd - 
    #
    #      if not defined SPARKCLR_JAR (set SPARKCLR_JAR=spark-clr_2.10-1.5.2-SNAPSHOT.jar)
    # 
    Get-ChildItem $targetDir -filter "sparkclr-submit.cmd" -recurs | % { 
        Write-Output "[SetSparkClrJarVersion.Update-SparkClrSubmit] updating $($_.FullName)"
        ((Get-Content $_.FullName) -replace "\(set SPARKCLR_JAR=.*\)", "(set SPARKCLR_JAR=spark-clr_2.10-$version.jar)") | Set-Content $_.FullName -force
    }

    Write-Output "[SetSparkClrJarVersion.Update-SparkClrSubmit] Done setting sparkclr-submit.cmd under $targetDir to version=$version"

    Write-Output "[SetSparkClrJarVersion.Update-SparkClrSubmit] Start setting sparkclr-submit.sh under $targetDir to version=$version"

    # 
    #  Update Mobius package version to this release. The version string in sparkclr-submit.sh - 
    #
    #      export SPARKCLR_JAR=spark-clr_2.10-1.6.0-SNAPSHOT.jar
    # 
    Get-ChildItem $targetDir -filter "sparkclr-submit.sh" -recurs | % { 
        Write-Output "[SetSparkClrJarVersion.Update-SparkClrSubmit] updating $($_.FullName)"
        ((Get-Content $_.FullName) -replace "export SPARKCLR_JAR=.*", "export SPARKCLR_JAR=spark-clr_2.10-$version.jar") | Set-Content $_.FullName -force
    }

    Write-Output "[SetSparkClrJarVersion.Update-SparkClrSubmit] Done setting sparkclr-submit.sh under $targetDir to version=$version"
}

function Print-Usage
{
    Write-Output '====================================================================================================='
    Write-Output ''
    Write-Output '    This script takes in "version" (required) and "targetDir" (optional) parameters, update Spark-CLR jar '
    Write-Output '    version reference in all scripts under "targetdir". '
    Write-Output ''
    Write-Output '    "targetDir" parameter is default to current directory where this script is located. '
    Write-Output ''
    Write-Output '    Example usage - '
    Write-Output '        powershell -f SetSparkClrJarVersion.ps1 -version 1.5.200-preview-1'
    Write-Output ''
    Write-Output '====================================================================================================='
}

#
# main body of the script
#
if (!$PSBoundParameters.ContainsKey('version') -or [string]::IsNullOrEmpty($version))
{
    Print-Usage
    return
}

if (!$PSBoundParameters.ContainsKey('targetDir') -or [string]::IsNullOrEmpty($targetDir))
{
    $targetDir = Get-ScriptDirectory
    Write-Output "[SetSparkClrJarVersion] targetDir is set to $targetDir"
}

Update-SparkClrSubmit $targetDir $version
