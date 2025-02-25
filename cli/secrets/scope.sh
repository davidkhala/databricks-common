create() {
  local scope_name=${1:-scope}
  databricks secrets create-scope "$scope_name"
}
list() {
  databricks secrets list-scopes
}
delete() {
  local scope_name=$1
  databricks secrets delete-scope "$scope_name"
}
"$@"
