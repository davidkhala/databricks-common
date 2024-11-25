auth() {
    databricks auth describe
}
metastores() {
    databricks metastores current $@
}
metastore() {
    metastores | jq -r .metastore_id
}
user() {
    databricks current-user me
}

$@
