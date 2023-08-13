$requirementsFile = "infrastructure/modules/lambda/requirements.txt"
$installDir = "infrastructure/modules/lambda/layers"

# Create the installation directory if it doesn't exist
if (-not (Test-Path $installDir)) {
  New-Item -ItemType Directory -Path $installDir | Out-Null
}

# Read the requirements file and install each library
Get-Content $requirementsFile | ForEach-Object {
  $libName = $_.Split("=")[0].Trim()
  $libDir = Join-Path $installDir $libName

  # Skip the installation if the ZIP file already exists
  if (Test-Path $zipFile) {
    Write-Host "Skipping installation of $libName because $zipFile already exists."
    return
  }

  # Create the library directory if it doesn't exist
  if (-not (Test-Path $libDir)) {
    New-Item -ItemType Directory -Path $libDir | Out-Null
  }

  # Install the library in the library directory
  pip install $_ -t (Join-Path $libDir "python")

  # Zip the library directory
#   $zipFile = Join-Path $installDir "$libName.zip"
#   Compress-Archive -Path $libDir -DestinationPath $zipFile
}

## For pscyopg2 layer, I used it from this git repo
# https://github.com/jkehler/awslambda-psycopg2/tree/master