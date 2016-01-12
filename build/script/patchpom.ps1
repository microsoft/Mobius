#
# Input -
#     "targetPom" parameter, target Pom.xml file
#
Param([string] $targetPom)


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
#
#
if (!(test-path $targetPom))
{
    Write-Output "[patchpom.ps1] [WARNING] $targetPom does not exist. Abort."
    Print-Usage
    return
}

#
# enable "<scope>provided</scope>" to exclude run-time spark packages from uber package
#
$content = (Get-Content $targetPom)
$content = $content -replace '<!--\s*<scope>provided</scope>\s*-->', '<scope>provided</scope>'

$content | Set-Content $targetPom -force

Write-Output "[patchpom.ps1] updated $targetPom"

    
