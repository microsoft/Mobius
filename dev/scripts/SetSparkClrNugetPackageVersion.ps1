#
# This script takes in and "nuspecDir" and "version" parameters, update Mobius Nuget package 
# version
#
Param([string]$nuspecDir, [string]$version)

if (!(test-path $nuspecDir))
{
	Write-Output "[SetSparkClrNugetPackageVersion] WARNING!!! $nuspecDir does not exist. Please provide a valid directory name !"
	return
}

# Nuget does not support version format 1.6.0-SNAPSHOT.1, need to change to 1.6.0-SNAPSHOT-1
$validNugetVersion = $version -replace "(\s*\S*-\S*)(\.)(\S*)",'$1-$3'

Write-Output "[SetSparkClrNugetPackageVersion] Start setting SparkCLR.nuspec under $nuspecDir to version=$validNugetVersion"

# 
#  Update Mobius package version. Example in SparkCLR.nuspec:  
#      <version>1.6.0-SNAPSHOT-1</version>
# 
Get-ChildItem $nuspecDir -filter "SparkCLR.nuspec" | % { 
	Write-Output "[SetSparkClrNugetPackageVersion] updating $($_.FullName)"
	((Get-Content $_.FullName) -replace "<version>\s*\S*</version>", "<version>$validNugetVersion</version>") | Set-Content -Encoding UTF8 -Path $_.FullName -force
}

Write-Output "[SetSparkClrNugetPackageVersion] Done setting SparkCLR.nuspec under $nuspecDir to version=$validNugetVersion"

