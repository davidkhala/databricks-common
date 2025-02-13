set -e
install() {
  curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sudo sh
}
login-serverless() {
  local workspace=$1 # workspace instance name
  local pat=$2 # Personal Access Token
  export DATABRICKS_SERVERLESS_COMPUTE_ID='auto' # not harm for profile file
  if [ -n "$2" ]; then
    databricks configure --token --host "https://$workspace" <<<"$pat"
  else
    databricks configure
  fi
  curl -s https://raw.githubusercontent.com/davidkhala/linux-utils/refs/heads/main/editors.sh | bash -s configure "serverless_compute_id=auto" ~/.databrickscfg

}
login() {
  local workspace=$1 # workspace instance name.
  local pat=$2 # Personal access token
  if [ -n "$3" ]; then
    export DATABRICKS_CLUSTER_ID=$3
    databricks configure --token --host "https://$workspace" <<<"$pat"
  else
    # interactive auto cluster_id discovery
    databricks configure --configure-cluster
  fi

}
logout() {
  rm ~/.databrickscfg
}
"$@"
