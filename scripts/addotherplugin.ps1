#
# Input - 
#     (1) "targetPom" parameter, target Pom.xml file
#     (1) "sourcePom" parameter, has other plug-in definitions in XML
#     (2) "variable" parameter, which exists in "targetPom", to be replaced with content of "sourcePom"
#
Param([string] $targetPom, [string] $sourcePom, [string] $variable)


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
    Write-Output "[addotherplugin.Replace-VariableInFile] variable=$variable, sourceFile=$sourceFile, targetFile=$targetFile"
    if (!(test-path $sourceFile))
    {
        Write-Output "[addotherplugin.Replace-VariableInFile] [WARNING] $sourceFile does not exist. Abort."
        return
    }

    if ([string]::IsNullOrEmpty($variable))
    {
        Write-Output "[addotherplugin.Replace-VariableInFile] [WARNING] variable name is empty. Abort."
        return
    }

    $now = Get-Date
    Write-Host "[Replace-VariableInFile] [$now] replace $variable in $sourceFile to produce $targetFile"
    (get-content $sourceFile) | Foreach-Object {
        $_ -replace "$variable", $value `
        } | Set-Content $targetFile -force
}


#
# main body of the script
#
#
if (!(test-path $targetPom))
{
    Write-Output "[addotherplugin.ps1] [WARNING] $targetPom does not exist. Abort."
    Print-Usage
    return
}

if (!(test-path $sourcePom))
{
    Write-Output "[addotherplugin.ps1] [WARNING] $sourcePom does not exist. Abort."
    Print-Usage
    return
}

if (!($PSBoundParameters.ContainsKey('variable')))
{
    Print-Usage
    return
}

$temp = [Environment]::GetEnvironmentVariable("temp").ToLower()

$otherPlugin = [Io.File]::ReadAllText($sourcePom)
$tempPom = "$temp\$targetPom"

Replace-VariableInFile $variable $otherPlugin $targetPom $tempPom

#
# enable "<scope>provided</scope>" to exclude run-time spark packages from uber package
#
$content = (Get-Content $tempPom)
$content = $content -replace '<!--\s*<scope>provided</scope>\s*-->', '<scope>provided</scope>'

$content | Out-File $targetPom -force

Write-Output "[addotherplugin.ps1] updated $targetPom"

    
