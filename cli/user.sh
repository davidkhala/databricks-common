group() {
    databricks groups list -o json $@
}
admins() {
    # group "admins"
    group | jq -r '.[] | select(.displayName=="admins") | .id'
}
users() {
    # group "users"
    group | jq -r '.[] | select(.displayName=="users") | .id'
}

create-service-principal() {
    local name=$1
    shift 1
    databricks service-principals create --active --display-name $name $@ | jq '{applicationId,id}'
}
list-service-principal() {
    databricks service-principals list -o json
}
$@
