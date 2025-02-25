list() {
    # artifact allowlist
    echo ">>> init script"
    databricks artifact-allowlists get INIT_SCRIPT

    echo ">>> JAR"
    databricks artifact-allowlists get LIBRARY_JAR

    echo ">>> maven"
    databricks artifact-allowlists get LIBRARY_MAVEN
}
script() {
    databricks artifact-allowlists update INIT_SCRIPT --json "{\"artifact_matchers\": [{\"artifact\": \"$@\",\"match_type\": \"PREFIX_MATCH\"}]}"
}
jar() {
    databricks artifact-allowlists update LIBRARY_JAR --json "{\"artifact_matchers\": [{\"artifact\": \"$@\",\"match_type\": \"PREFIX_MATCH\"}]}"
}
maven() {
    databricks artifact-allowlists update LIBRARY_MAVEN --json "{\"artifact_matchers\": [{\"artifact\": \"$@\",\"match_type\": \"PREFIX_MATCH\"}]}"
}

"$@"
