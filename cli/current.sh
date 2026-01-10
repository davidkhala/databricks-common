metastores() {
  databricks metastores current $@
}
metastore() {
  metastores | jq -r .metastore_id
}
user() {
  databricks current-user me
}
config() {
  databricks auth describe
}

"$@"
