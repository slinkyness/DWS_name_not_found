## build_lambdas.ps1
# ==========================================
# Packages each Lambda into a deployment ZIP
# and creates a Lambda Layer ZIP for dependencies.
#
# Requirements:
#   - Python 3.x on PATH  (python or python3)
#   - pip on PATH
#   - Run from the folder containing this script
#
# Usage:
#   .\build_lambdas.ps1
#   .\build_lambdas.ps1 -Only news        # build just one Lambda
#
# Output:
#   dist\<n>.zip              one file per Lambda (code only, no dependencies)
#   dist\lambda-layer.zip     Lambda Layer with all dependencies
#
# Each Lambda ZIP contains:
#   lambda_function.py   Handler entry point (renamed from <n>_function.py)
#   <n>_fetch.py         Fetch logic
#
# Lambda Layer ZIP contains:
#   python/                  Python packages directory (required for Lambda Layers)
#     <all dependencies>     Installed third-party packages (requests, boto3, ...)
#     lambda_utils.py        Shared helpers (available to all functions)
#
# AWS Lambda handler string for all functions: lambda_function.lambda_handler
#
# To use:
#   1. Create a layer in AWS Lambda console or CLI:
#      aws lambda publish-layer-version --layer-name my-deps \
#        --zip-file fileb://dist/lambda-layer.zip \
#        --compatible-runtimes python3.11
#   2. Attach the layer to each Lambda function
#   3. Deploy function code ZIPs as usual
# ==========================================

param(
    [string]$Only = "",      # optional: build only this Lambda name
    [string]$Arch = "arm64", # Lambda CPU: x86_64 (default) or arm64 (Graviton)
    [string]$Utils = "lambda_layers\lambda_utils.py"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# -- Validate architecture -----------------------------------------------------
$ValidArchs = @("x86_64", "arm64")
if ($Arch -notin $ValidArchs) {
    throw "Invalid -Arch '$Arch'. Valid values: $($ValidArchs -join ', ')"
}
$PipPlatform   = "linux_$Arch"   # e.g. linux_x86_64 or linux_arm64
$PythonVersion = "3.11"          # must match your Lambda runtime

# -- Resolve python command ----------------------------------------------------
$PythonCmd = if (Get-Command python  -ErrorAction SilentlyContinue) { "python"  }
             elseif (Get-Command python3 -ErrorAction SilentlyContinue) { "python3" }
             else { throw "Python not found on PATH. Install Python 3 and retry." }

Write-Host "`nUsing Python : $PythonCmd  ($(& $PythonCmd --version 2>&1))"
Write-Host "Target arch  : $PipPlatform  (Python $PythonVersion)"

# -- Locate lambda_utils.py  ------
$LambdaUtilsPath = $null
$SearchDir = (Get-Location).Path
while ($SearchDir -ne "") {
    $Candidate = Join-Path $SearchDir $Utils
    if (Test-Path $Candidate) {
        $LambdaUtilsPath = $Candidate
        break
    }
    $Parent = Split-Path $SearchDir -Parent
    if ($Parent -eq $SearchDir) { break }   # reached filesystem root
    $SearchDir = $Parent
}
if (-not $LambdaUtilsPath) {
    throw "Could not find utils\lambda_utils.py in '$PSScriptRoot' or any parent directory."
}
Write-Host "Found lambda_utils : $LambdaUtilsPath`n"

# -- Locate requirements.txt ----------
$RequirementsPath = $null

while ($SearchDir -ne "") {
    $Candidate = Join-Path $SearchDir "requirements.txt"
    if (Test-Path $Candidate) {
        $RequirementsPath = $Candidate
        break
    }
    $Parent = Split-Path $SearchDir -Parent
    if ($Parent -eq $SearchDir) { break }   # reached filesystem root
    $SearchDir = $Parent
}
if (-not $RequirementsPath) {
    throw "Could not find requirements.txt in '$PSScriptRoot' or any parent directory."
}
Write-Host "Found requirements  : $RequirementsPath`n"

# -- Lambda definitions --------------------------------------------------------
$Lambdas = [ordered]@{
    "news"     = @("news_function.py",    "news_fetch.py")
    "currents" = @("current_function.py", "current_fetch.py")
    "core"     = @("core_function.py",  "core_fetch.py")
    "ct_us"    = @("ct_us_function.py",   "ct_us_fetch.py")
    "who"      = @("who_function.py",     "who_fetch.py")
    "gho"      = @("gho_function.py",     "gho_fetch.py")
    "nih"      = @("nih_function.py",     "nih_fetch.py")
}

# -- Filter if -Only was passed ------------------------------------------------
if ($Only -ne "") {
    if (-not $Lambdas.Contains($Only)) {
        throw "Unknown Lambda name '$Only'. Valid names: $($Lambdas.Keys -join ', ')"
    }
    $Lambdas = [ordered]@{ $Only = $Lambdas[$Only] }
    Write-Host "Building only: $Only`n"
}

# -- Paths ---------------------------------------------------------------------
$ScriptDir = (Get-Location).Path   # CWD: the folder you're running from (e.g. prod/fetch)
$DistDir   = Join-Path $ScriptDir "dist"
$BuildDir  = Join-Path $ScriptDir ".build_tmp"

New-Item -ItemType Directory -Force -Path $DistDir  | Out-Null
New-Item -ItemType Directory -Force -Path $BuildDir | Out-Null

# -- Install dependencies into a layer package dir ----------------------------
$LayerPkgDir = Join-Path (Join-Path $BuildDir "layer") "python"
Write-Host "Installing dependencies for Lambda Layer into $LayerPkgDir ..."
& $PythonCmd -m pip install `
    --quiet `
    --target $LayerPkgDir `
    --platform $PipPlatform `
    --implementation cp `
    --python-version $PythonVersion `
    --only-binary=:all: `
    --ignore-installed `
    --requirement $RequirementsPath

# -- Add shared utils to layer --------------------------------------------------
Write-Host "Adding lambda_utils.py to layer ..."
Copy-Item $LambdaUtilsPath -Destination $LayerPkgDir
Write-Host "  + lambda_utils.py  (from $LambdaUtilsPath)`n"

# -- Build Lambda Layer ZIP ====================================================
Add-Type -AssemblyName System.IO.Compression.FileSystem

$LayerZipPath = Join-Path $DistDir "lambda-layer.zip"
$LayerDir     = Join-Path $BuildDir "layer"

Write-Host "Building lambda-layer.zip ..."

if (Test-Path $LayerZipPath) { Remove-Item $LayerZipPath -Force }
[System.IO.Compression.ZipFile]::CreateFromDirectory($LayerDir, $LayerZipPath)

$LayerSizeMB = [math]::Round((Get-Item $LayerZipPath).Length / 1MB, 1)
Write-Host "  -> dist\lambda-layer.zip  ($LayerSizeMB MB)`n"

# -- Build each Lambda ZIP (code only) =========================================

foreach ($Name in $Lambdas.Keys) {
    $Files    = $Lambdas[$Name]
    $ZipPath  = Join-Path $DistDir "$Name.zip"
    $StageDir = Join-Path $BuildDir $Name

    Write-Host "Building $Name.zip ..."

    if (Test-Path $StageDir) { Remove-Item $StageDir -Recurse -Force }
    New-Item -ItemType Directory -Force -Path $StageDir | Out-Null

    $IsFirst = $true
    foreach ($File in $Files) {
        $Src = Join-Path $ScriptDir $File
        if (-not (Test-Path $Src)) {
            throw "  ERROR: source file not found: $Src"
        }
        if ($IsFirst) {
            Copy-Item $Src -Destination (Join-Path $StageDir "lambda_function.py")
            Write-Host "  + $File  ->  lambda_function.py"
            $IsFirst = $false
        } else {
            Copy-Item $Src -Destination $StageDir
            Write-Host "  + $File"
        }
    }

    if (Test-Path $ZipPath) { Remove-Item $ZipPath -Force }
    [System.IO.Compression.ZipFile]::CreateFromDirectory($StageDir, $ZipPath)

    $SizeMB = [math]::Round((Get-Item $ZipPath).Length / 1MB, 1)
    Write-Host "  -> dist\$Name.zip  ($SizeMB MB)`n"
}

# -- Summary -------------------------------------------------------------------
Write-Host "========================================"
Write-Host "All ZIPs written to: $DistDir"
Write-Host ""
Write-Host "Handler string for all Lambdas:  lambda_function.lambda_handler"
Write-Host ""
Write-Host "Deploy with AWS CLI:"
Write-Host ""
Write-Host "1. Create the Lambda Layer (once):"
Write-Host "     aws lambda publish-layer-version --layer-name my-deps \"
Write-Host "       --zip-file fileb://dist/lambda-layer.zip \"
Write-Host "       --compatible-runtimes python3.11"
Write-Host ""
Write-Host "2. For each Lambda function:"
Write-Host "     aws lambda create-function --function-name <name> \"
Write-Host "       --runtime python3.11 ``"
Write-Host "       --role <your-role-arn> ``"
Write-Host "       --handler lambda_function.lambda_handler ``"
Write-Host "       --zip-file fileb://dist/<name>.zip ``"
Write-Host "       --layers <layer-arn>"
Write-Host ""
Write-Host "   Or update existing function:"
Write-Host "     aws lambda update-function-code ``"
Write-Host "       --function-name <your-function-name> ``"
Write-Host "       --zip-file fileb://dist/<name>.zip"
Write-Host ""
Write-Host "   And attach the layer:"
Write-Host "     aws lambda update-function-configuration ``"
Write-Host "       --function-name <your-function-name> ``"
Write-Host "       --layers <layer-arn>"
Write-Host "========================================"

# -- Cleanup -------------------------------------------------------------------
Remove-Item $BuildDir -Recurse -Force
Write-Host "Build temp dir cleaned up."