# UTF-8 BOM for Windows PowerShell 5.1 and Chinese paths.
[CmdletBinding()]
param([string]$Pythonw)
$ErrorActionPreference = 'Stop'
if (-not $Pythonw) {
    $Pythonw = (& python -c "import pathlib,sys; print(pathlib.Path(sys.executable).with_name('pythonw.exe'))")
    if ($LASTEXITCODE -ne 0) { throw '无法找到 Python，请用 -Pythonw 指定 pythonw.exe 的完整路径。' }
}
$Pythonw = (Resolve-Path -LiteralPath $Pythonw).Path
if (-not (Test-Path -LiteralPath $Pythonw -PathType Leaf)) { throw 'pythonw.exe 不存在。' }
$entry = Join-Path $PSScriptRoot 'bunkr-app.pyw'
if (-not (Test-Path -LiteralPath $entry -PathType Leaf)) { throw '找不到 bunkr-app.pyw。' }
$desktop = [Environment]::GetFolderPath('Desktop')
$linkPath = Join-Path $desktop 'Bunkr 下载.lnk'
$shell = New-Object -ComObject WScript.Shell
$link = $shell.CreateShortcut($linkPath)
$link.TargetPath = $Pythonw
$link.Arguments = '"' + $entry + '"'
$link.WorkingDirectory = $PSScriptRoot
$link.Description = '打开 Bunkr 下载桌面窗口'
$link.Save()
$check = $shell.CreateShortcut($linkPath)
if (-not (Test-Path -LiteralPath $check.TargetPath -PathType Leaf)) { throw '快捷方式自检失败：TargetPath 不存在。' }
if ($check.TargetPath -ne $Pythonw -or $check.Arguments -ne ('"' + $entry + '"')) { throw '快捷方式自检失败：启动路径不一致。' }
Write-Host "已创建并检查：$linkPath"
