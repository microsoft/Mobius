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
# main body of the script
# this script copies jar file for the release
#
$scriptDir= Get-ScriptDirectory
write-output "Script directory: $scriptDir"
$destDir = "$scriptDir\runtime\lib"
write-output "Directory to which file will be copied to: $destDir"
pushd ..\scala\target

#non-uber jar has original prefix - this is the file that needs to be copied over
$files = get-childitem $configPath -filter "original*"

#only one file in $files
foreach($file in $files)
{
	$sourceFileName = $file.Name
	write-output "Name of the file to copy: $sourceFileName"
}

$pattern = "^original-(.*)"
$destFileName = $sourceFileName -replace $pattern,'$1'
write-output "Name of the file to use in destination: $destFileName"

copy-item $sourceFileName -Destination "$destDir\$destFileName"
popd
