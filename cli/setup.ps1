function install(){
    winget install --accept-source-agreements Databricks.DatabricksCLI
}

function logout(){
    rm $env:USERPROFILE/.databrickscfg
}