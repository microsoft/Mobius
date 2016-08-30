#
# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for full license information.
#

$x64items = @(Get-ChildItem "HKLM:SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall")
$x64items + @(Get-ChildItem "HKLM:SOFTWARE\wow6432node\Microsoft\Windows\CurrentVersion\Uninstall") `
   | ForEach-object { Get-ItemProperty Microsoft.PowerShell.Core\Registry::$_ } `
   | Sort-Object -Property DisplayName `
   | Select-Object -Property DisplayName,DisplayVersion

Write-Host("x64Items = $xt4items")
