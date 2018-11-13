#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for full license information.
#

$root = (split-path -parent $MyInvocation.MyCommand.Definition) + '\..\..'

# expected tagname: v{version-string}. E.g., "v1.5.2-snapshot-2", "v1.5.2-prerelease-1"
$tagName = $env:APPVEYOR_REPO_TAG_NAME
Write-Host "[nugetpack.ps1] [INFO] tagname=[$tagName]"

if (($tagName.Length -gt 1) -and ($tagName.SubString(0,1).ToLower() -eq "v"))
{
    $len = $tagName.Length - 1
    $versionStr = $tagName.SubString(1, $len)

    Write-Host "[nugetpack.ps1] [INFO] Setting .nuspec version tag to $versionStr"

    $content = (Get-Content $root\csharp\SparkCLR.nuspec)
    $content = $content -replace "<version>.*</version>", "<version>$versionStr</version>"
    
    $content | Out-File $root\csharp\SparkCLR.compiled.nuspec
}
else
{
    Copy-Item $root\csharp\SparkCLR.nuspec $root\csharp\SparkCLR.compiled.nuspec -force
}

& $root\build\tools\NuGet.exe pack $root\csharp\SparkCLR.compiled.nuspec -Symbols
