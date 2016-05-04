#
# This script takes in "version" and "targetDir" (optional) parameters, update Mobius Nuget package 
# version reference in all *.csproj and packages.config under "dir". 
#
# "targetDir" parameter is default to current directory where this script is located, when not provided.
#
Param([string]$targetDir, [string]$version, [string]$nuspecDir, [string]$mode)

function Update-Csproj($targetDir, $version)
{
    if (!(test-path $targetDir))
    {
        Write-Output "[SetSparkClrPackageVersion.Update-Csproj] WARNING!!! $targetDir does not exist. Please provide a valid directory name !"
        return
    }

    Write-Output "[SetSparkClrPackageVersion.Update-Csproj] Start setting *.csproj under $targetDir to version=$version"

    # 
    # Update Mobius package version to this release. Example in *.csproj:  
    #     <HintPath>..\packages\Microsoft.SparkCLR.1.5.2-SNAPSHOT\lib\net45\CSharpWorker.exe</HintPath>
    # 
    Get-ChildItem $targetDir -filter "*.csproj" -recurs | % { 
        Write-Output "[SetSparkClrPackageVersion.Update-Csproj] updating $($_.FullName)"
 		((Get-Content $_.FullName) -replace "\\Microsoft\.SparkCLR.*\\lib", "\Microsoft.SparkCLR.$version\lib") | Set-Content -Encoding UTF8 -Path $_.FullName -force
	}

    Write-Output "[SetSparkClrPackageVersion.Update-Csproj] Done setting *.csproj under $targetDir to version=$version"
}

function Update-PackageConfig($targetDir, $version)
{
    if (!(test-path $targetDir))
    {
        Write-Output "[SetSparkClrPackageVersion.Update-PackageConfig] WARNING!!! $targetDir does not exist. Please provide a valid directory name !"
        return
    }

    Write-Output "[SetSparkClrPackageVersion.Update-PackageConfig] Start setting packages.config under $targetDir to version=$version"

    # 
    #  Update Mobius package version to this release. Example in packages.config:  
    #      <package id="Microsoft.SparkCLR" version="1.5.2-SNAPSHOT" targetFramework="net45" />
    # 
    Get-ChildItem $targetDir -filter "packages.config" -recurs | % { 
        Write-Output "[SetSparkClrPackageVersion.Update-PackageConfig] updating $($_.FullName)"
        ((Get-Content $_.FullName) -replace "`"Microsoft\.SparkCLR`"\s*version=\S*\s", "`"Microsoft.SparkCLR`" version=`"$version`" ") | Set-Content -Encoding UTF8 -Path $_.FullName -force
    }

    Write-Output "[SetSparkClrPackageVersion.Update-PackageConfig] Done setting *.csproj under $targetDir to version=$version"
}

function Update_NuSpec($nuspecDir, $version)
{
    if (!(test-path $nuspecDir))
    {
        Write-Output "[SetSparkClrPackageVersion.Update-NuSpec] WARNING!!! $nuspecDir does not exist. Please provide a valid directory name !"
        return
    }

    Write-Output "[SetSparkClrPackageVersion.Update-NuSpec] Start setting SparkCLR.nuspec under $nuspecDir to version=$version"

    # 
    #  Update Mobius package version to this release. Example in SparkCLR.nuspec:  
    #      <version>1.5.2-SNAPSHOT</version>
    # 
    Get-ChildItem $nuspecDir -filter "SparkCLR.nuspec" | % { 
        Write-Output "[SetSparkClrPackageVersion.Update-NuSpec] updating $($_.FullName)"
        ((Get-Content $_.FullName) -replace "<version>\s*\S*</version>", "<version>$version</version>") | Set-Content -Encoding UTF8 -Path $_.FullName -force
    }

    Write-Output "[SetSparkClrPackageVersion.Update-NuSpec] Done setting SparkCLR.nuspec under $nuspecDir to version=$version"
}

function Print-Usage
{
    Write-Output '====================================================================================================='
    Write-Output ''
    Write-Output '    This script takes in "version" (required) and "targetDir" (optional) parameters, update SparkCLR '
    Write-Output '    Nuget package version reference in all *.csproj and packages.config under "targetdir". '
    Write-Output ''
    Write-Output '    "targetDir" parameter is default to current directory where this script is located. '
    Write-Output ''
    Write-Output '    "mode" parameter is used to update version in Example projects or core artifacts like nuspec.'
	Write-Output '    Mode options are "examples" and "core" respectively'
    Write-Output ''	
    Write-Output '    Example usage - '
    Write-Output '        powershell -f SetSparkClrPackageVersion.ps1 -version 1.5.200-preview-1' -mode [core|examples]
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
	Print-Usage
    return
}

if (!$PSBoundParameters.ContainsKey('nuspecDir') -or [string]::IsNullOrEmpty($nuspecDir))
{
	Print-Usage
    return
}

if (!$PSBoundParameters.ContainsKey('mode') -or [string]::IsNullOrEmpty($mode))
{
	Print-Usage
    return
}

if ($mode -eq "examples") {
	Update-Csproj $targetDir $version
	Update-PackageConfig $targetDir $version
} elseif ($mode -eq "core") {
	Update_NuSpec $nuspecDir $version
}
