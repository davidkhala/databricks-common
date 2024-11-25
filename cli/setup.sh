install() {
  curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sudo sh
}
login() {
  local url=$1 # workspace url. in format of `adb-662901427557763.3.azuredatabricks.net`
  local pat=$2 # Personal access token

  databricks configure --token --host https://$url <<<$pat
}
logout() {
  rm ~/.databrickscfg
}
$@
