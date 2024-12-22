list() {
    databricks groups list -o json
}
admins() {
    # group "admins"
    list | jq -r '.[] | select(.displayName=="admins") | .id'
}
users() {
    # group "users"
    list | jq -r '.[] | select(.displayName=="users") | .id'
}
"$@"