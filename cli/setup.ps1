function install(){
    winget install --accept-package-agreements Databricks.DatabricksCLI
}

function logout(){
    rm $env:USERPROFILE/.databrickscfg
}