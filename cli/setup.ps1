$ErrorActionPreference = "Stop"

function Install
{
    winget install --accept-source-agreements Databricks.DatabricksCLI
}

function Logout
{
    Remove-Item $env:USERPROFILE/.databrickscfg
}

function Login-Serverless
{
    param (
        [string]$workspace, # workspace instance name
        [string]$pat # Personal access token
    )

    # Set environment variable
    $env:DATABRICKS_SERVERLESS_COMPUTE_ID = 'auto'

    # Configure Databricks CLI
    if ($pat)
    {
        # Use the PAT to configure Databricks CLI
        echo $pat | databricks configure --token --host "https://$workspace"
    }
    else
    {
        # Run Databricks CLI configuration without PAT
        databricks configure
    }

    (Invoke-WebRequest "https://raw.githubusercontent.com/davidkhala/windows-utils/refs/heads/master/editor.ps1" -UseBasicParsing).Content | Invoke-Expression
    Append $env:USERPROFILE/.databrickscfg serverless_compute_id=auto
}
Invoke-Expression ($args -join " ")