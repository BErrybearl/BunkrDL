#Requires -Version 7.0
<#
仓库检查门槛：提交前运行，任一项不通过则退出码非 0。
用法：pwsh -NoProfile -File scripts/check.ps1
检查项：
  [a] 根 AGENTS.md 含规则定界标记，CLAUDE.md 指向 AGENTS.md
  [b] 已跟踪文件不含个人绝对路径与密钥形状
  [c] 已跟踪文件不含产物 denylist 中的路径
  [d] .githooks/pre-commit（若存在）调用了本脚本
#>

$ErrorActionPreference = 'Stop'

function Get-RepoRoot {
    $root = & git rev-parse --show-toplevel 2>$null
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($root)) {
        Write-Host 'FAIL  [a] not inside a git work tree (git rev-parse --show-toplevel failed)'
        exit 2
    }
    return ($root | Select-Object -First 1).Trim()
}

$Root = Get-RepoRoot
Set-Location -LiteralPath $Root

$script:Failed = 0
function Write-Item {
    param([bool]$Ok, [string]$Label, [string[]]$Detail = @())
    if ($Ok) {
        Write-Host "PASS  $Label"
    } else {
        Write-Host "FAIL  $Label"
        $script:Failed++
    }
    foreach ($line in $Detail) { Write-Host "      $line" }
}

# ---------------------------------------------------------------- [a]
$aOk = $true
$aDetail = @()
$beginMark = '<!-- vibe-rules:begin -->'
$endMark = '<!-- vibe-rules:end -->'
$agentsPath = Join-Path $Root 'AGENTS.md'
if (-not (Test-Path -LiteralPath $agentsPath -PathType Leaf)) {
    $aOk = $false
    $aDetail += 'AGENTS.md: not found at repo root'
} else {
    $agentsText = Get-Content -LiteralPath $agentsPath -Raw
    if ([string]::IsNullOrEmpty($agentsText)) { $agentsText = '' }
    foreach ($mark in @($beginMark, $endMark)) {
        if ($agentsText.IndexOf($mark, [StringComparison]::Ordinal) -lt 0) {
            $aOk = $false
            $aDetail += "AGENTS.md: rule marker missing: $mark"
        }
    }
    $iBegin = $agentsText.IndexOf($beginMark, [StringComparison]::Ordinal)
    $iEnd = $agentsText.IndexOf($endMark, [StringComparison]::Ordinal)
    if ($iBegin -ge 0 -and $iEnd -ge 0 -and $iEnd -lt $iBegin) {
        $aOk = $false
        $aDetail += 'AGENTS.md: rule markers out of order (end before begin)'
    }
}
$claudePath = Join-Path $Root 'CLAUDE.md'
if (-not (Test-Path -LiteralPath $claudePath -PathType Leaf)) {
    $aOk = $false
    $aDetail += 'CLAUDE.md: not found at repo root'
} else {
    $claudeText = Get-Content -LiteralPath $claudePath -Raw
    if ([string]::IsNullOrEmpty($claudeText)) { $claudeText = '' }
    if ($claudeText -notmatch '(?m)^\s*@AGENTS\.md\s*$') {
        $aOk = $false
        $aDetail += 'CLAUDE.md: missing line @AGENTS.md'
    }
}
Write-Item -Ok $aOk -Label '[a] AGENTS.md rule markers + CLAUDE.md @AGENTS.md' -Detail $aDetail

# ---------------------------------------------------------------- [b]
$PathAndSecretPatterns = @(
    @{ Label = 'windows user profile path'; Pattern = '[A-Za-z]:[\\/]Users[\\/][^\\/[:space:]]+' },
    @{ Label = 'macos user profile path';   Pattern = '/Users/[^/[:space:]]+' },
    @{ Label = 'linux user profile path';   Pattern = '/home/[^/[:space:]]+' },
    @{ Label = 'github token shape';        Pattern = 'ghp_[A-Za-z0-9]{30,}' },
    @{ Label = 'api key shape (sk-)';       Pattern = 'sk-[A-Za-z0-9]{20,}' },
    @{ Label = 'aws access key shape';      Pattern = 'AKIA[0-9A-Z]{16}' },
    @{ Label = 'private key block';         Pattern = '-----BEGIN [A-Z ]*PRIVATE KEY-----' }
)
$bDetail = @()
foreach ($entry in $PathAndSecretPatterns) {
    $out = & git grep -I -n -E -e $entry.Pattern -- . ':!scripts/check.ps1' 2>$null
    $code = $LASTEXITCODE
    if ($code -eq 0) {
        foreach ($line in $out) { $bDetail += "[$($entry.Label)] $line" }
    } elseif ($code -gt 1) {
        $bDetail += "[$($entry.Label)] git grep failed (exit $code)"
    }
}
Write-Item -Ok ($bDetail.Count -eq 0) -Label '[b] tracked files: no personal paths / secret shapes' -Detail $bDetail

# ---------------------------------------------------------------- [c]
$ArtifactDenylist = @(
    @{ Name = '__pycache__/'; Match = @('*__pycache__/*') },
    @{ Name = '*.pyc';        Match = @('*.pyc') },
    @{ Name = 'dist/';        Match = @('dist/*', '*/dist/*') },
    @{ Name = 'build/';       Match = @('build/*', '*/build/*') },
    @{ Name = 'node_modules/';Match = @('node_modules/*', '*/node_modules/*') },
    @{ Name = '*.log';        Match = @('*.log') },
    @{ Name = '*.sqlite';     Match = @('*.sqlite') },
    @{ Name = '*.db';         Match = @('*.db') },
    @{ Name = '.env';         Match = @('.env', '*/.env', '.env.*', '*/.env.*') }
)
$tracked = @(& git ls-files)
if ($LASTEXITCODE -ne 0) {
    Write-Item -Ok $false -Label '[c] tracked files: no build artifacts / local state' -Detail @("git ls-files failed (exit $LASTEXITCODE)")
} else {
    $cDetail = @()
    foreach ($file in $tracked) {
        foreach ($entry in $ArtifactDenylist) {
            foreach ($glob in $entry.Match) {
                if ($file -like $glob) {
                    $cDetail += "[$($entry.Name)] $file"
                    break
                }
            }
        }
    }
    Write-Item -Ok ($cDetail.Count -eq 0) -Label '[c] tracked files: no build artifacts / local state' -Detail $cDetail
}

# ---------------------------------------------------------------- [d]
$dDetail = @()
$dNote = @()
$hookRel = '.githooks/pre-commit'
$hookPath = Join-Path $Root $hookRel
if (-not (Test-Path -LiteralPath $hookPath -PathType Leaf)) {
    $dNote += "${hookRel}: not present (skipped)"
} else {
    $hookText = Get-Content -LiteralPath $hookPath -Raw
    if ([string]::IsNullOrEmpty($hookText)) { $hookText = '' }
    if ($hookText -notmatch 'scripts/check\.ps1') {
        $dDetail += "${hookRel}: does not call scripts/check.ps1"
    }
}
Write-Item -Ok ($dDetail.Count -eq 0) -Label '[d] pre-commit hook calls scripts/check.ps1' -Detail ($dDetail + $dNote)

# ---------------------------------------------------------------- summary
Write-Host ''
if ($script:Failed -eq 0) {
    Write-Host 'CHECK OK (4/4 checks passed)'
    exit 0
}
Write-Host "CHECK FAILED ($($script:Failed) of 4 checks failed)"
exit 1
