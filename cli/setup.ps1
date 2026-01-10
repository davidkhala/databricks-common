$ErrorActionPreference = "Stop"

function Install
{
    winget install --accept-source-agreements Databricks.DatabricksCLI
}
function Upgrade
{
    winget upgrade Databricks.DatabricksCLI
}
function Logout
{
    Remove-Item $env:USERPROFILE/.databrickscfg
}

function Connect-Serverless
{
    # Login-Serverless
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
        Write-Output $pat | databricks configure --token --host "https://$workspace"
    }
    else
    {
        # Run Databricks CLI configuration without PAT
        databricks configure
    }
    databricks current-user me # validate
    (Invoke-WebRequest "https://raw.githubusercontent.com/davidkhala/windows-utils/refs/heads/master/editor.ps1" -UseBasicParsing).Content | Invoke-Expression
    Append $env:USERPROFILE/.databrickscfg serverless_compute_id=auto
}
function Configure-Account{
    param (
        [string]$account # account id
    )

    databricks auth login --host https://accounts.cloud.databricks.com --account-id $account
}
if ($args.Count -gt 0)
{
    Invoke-Expression ($args -join " ")
}