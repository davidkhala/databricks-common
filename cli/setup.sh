set -e
install() {
  curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sudo sh
}
login-serverless() {
  local url=$1                                   # workspace url. in format of `adb-662901427557763.3.azuredatabricks.net`
  local pat=$2                                   # Personal access token
  export DATABRICKS_SERVERLESS_COMPUTE_ID='auto' # not harm for profile file
  if [ -n "$2" ]; then
    databricks configure --token --host "https://$url" <<<"$pat"
  else
    databricks configure
  fi
  curl -s https://raw.githubusercontent.com/davidkhala/linux-utils/refs/heads/main/editors.sh | bash -s configure "serverless_compute_id=auto" ~/.databrickscfg

}
login() {
  local url=$1 # workspace url. in format of `adb-662901427557763.3.azuredatabricks.net`
  local pat=$2 # Personal access token
  if [ -n "$3" ]; then
    export DATABRICKS_CLUSTER_ID=$3
    databricks configure --token --host https://$url <<<$pat
  else
    # interactive auto cluster_id discovery
    databricks configure --configure-cluster
  fi

}
logout() {
  rm ~/.databrickscfg
}
"$@"
