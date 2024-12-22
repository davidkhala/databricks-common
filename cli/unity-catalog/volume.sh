# Unity Catalog volumes
set -e
schema=${schema:-default}
catalog=${catalog:-$(sed 's/-/_/' <<<$workspace_name)}
create-managed() {
    # create managed (local) volume
    local volume=$1
    # create if not exists
    if ! databricks volumes read "$catalog.$schema.$volume" >/dev/null; then
        databricks volumes create "$catalog" "$schema" "$volume" MANAGED
    fi
    databricks grants update volume "$catalog.$schema.$volume" --json '{"changes": [{"principal": "account users", "add": ["READ_VOLUME"]}]}' >/dev/null

}

list() {
    databricks volumes list "$catalog" "$schema"
}
"$@"
