scope=$2
list(){
  databricks secrets list-secrets "$scope"
}
put(){
  local key=$1
  local secret=$2
   databricks secrets put-secret "$scope" "$key" --string-secret "$secret"
}
delete(){
  local key=$1
  databricks secrets delete-secret "$scope" "$key"
}

"$@"