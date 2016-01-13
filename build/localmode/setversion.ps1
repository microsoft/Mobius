#
# This script takes in "dir" and "version" parameters, update SparkCLR Nuget package version reference 
# in all *.csproj and packages.config under "dir", recusively.
#
Param([string]$dir, [string]$version)

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

function Update-Csproj($sourceDir, $version)
{
    if (!(test-path $sourceDir))
    {
        Write-Output "[setversion.Update-Csproj] WARNING!!! $sourceDir does not exist. Please provide a valid directory name !"
        return
    }

    Write-Output "[setversion.Update-Csproj] Start setting *.csproj under $sourceDir to version=$version"

    # 
    # Update SparkCLR package version to this release. Example in *.csproj:  
    #     <HintPath>..\packages\Microsoft.SparkCLR.1.5.2-SNAPSHOT\lib\net45\CSharpWorker.exe</HintPath>
    # 
    Get-ChildItem $sourceDir -filter "*.csproj" -recurs | % { 
        Write-Output "[setversion.Update-Csproj] updating $($_.FullName)"
 		((Get-Content $_.FullName) -replace "\\Microsoft\.SparkCLR.*\\lib", "\Microsoft.SparkCLR.$version\lib") | Set-Content -Encoding UTF8 -Path $_.FullName -force
	}

    Write-Output "[setversion.Update-Csproj] Done setting *.csproj under $sourceDir to version=$version"
}

function Update-PackageConfig($sourceDir, $version)
{
    if (!(test-path $sourceDir))
    {
        Write-Output "[setversion.Update-PackageConfig] WARNING!!! $sourceDir does not exist. Please provide a valid directory name !"
        return
    }

    Write-Output "[setversion.Update-PackageConfig] Start setting packages.config under $sourceDir to version=$version"

    # 
    #  Update SparkCLR package version to this release. Example in packages.config:  
    #      <package id="Microsoft.SparkCLR" version="1.5.2-SNAPSHOT" targetFramework="net45" />
    # 
    Get-ChildItem $sourceDir -filter "packages.config" -recurs | % { 
        Write-Output "[setversion.Update-PackageConfig] updating $($_.FullName)"
        ((Get-Content $_.FullName) -replace "`"Microsoft\.SparkCLR`"\s*version=\S*\s", "`"Microsoft.SparkCLR`" version=`"$version`" ") | Set-Content -Encoding UTF8 -Path $_.FullName -force
    }

    Write-Output "[setversion.Update-PackageConfig] Done setting *.csproj under $sourceDir to version=$version"
}

function Print-Usage
{
    Write-Output '====================================================================================================='
    Write-Output ''
    Write-Output '    This script takes in "dir" and "version" parameters, update SparkCLR Nuget package version reference '
    Write-Output '    in all *.csproj and packages.config under "dir", recusively.'
    Write-Output ''
    Write-Output '====================================================================================================='
}

#
# main body of the script
#
if (!($PSBoundParameters.ContainsKey('dir')) -or !($PSBoundParameters.ContainsKey('version')))
{
    Print-Usage
    return
}

if (([string]::IsNullOrEmpty($dir)) -or ([string]::IsNullOrEmpty($version)))
{
    Print-Usage
    return
}


Update-Csproj $dir $version
Update-PackageConfig $dir $version

