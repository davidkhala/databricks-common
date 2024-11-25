# Unity Catalog
catelogs() {
    databricks catalogs list --include-browse
}
schemas() {
    local catalog=${1:-$catalog}
    databricks schemas list $catalog --include-browse
}
allowlist() {
    # artifact allowlist
    echo ">>> init script"
    databricks artifact-allowlists get INIT_SCRIPT

    echo ">>> JAR"
    databricks artifact-allowlists get LIBRARY_JAR

    echo ">>> maven"
    databricks artifact-allowlists get LIBRARY_MAVEN
}
allow-script() {
    databricks artifact-allowlists update INIT_SCRIPT --json "{\"artifact_matchers\": [{\"artifact\": \"$@\",\"match_type\": \"PREFIX_MATCH\"}]}"
}
allow-jar() {
    databricks artifact-allowlists update LIBRARY_JAR --json "{\"artifact_matchers\": [{\"artifact\": \"$@\",\"match_type\": \"PREFIX_MATCH\"}]}"
}
allow-maven() {
    databricks artifact-allowlists update LIBRARY_MAVEN --json "{\"artifact_matchers\": [{\"artifact\": \"$@\",\"match_type\": \"PREFIX_MATCH\"}]}"
}

$@
