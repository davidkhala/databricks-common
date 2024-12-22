create() {
    local name=$1
    shift 1
    databricks service-principals create --active --display-name "$name" "$@" | jq '{applicationId,id}'
}
list() {
    databricks service-principals list -o json
}
"$@"