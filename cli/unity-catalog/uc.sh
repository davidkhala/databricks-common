# Unity Catalog
catalogs() {
    databricks catalogs list --include-browse
}
schemas() {
    local catalog=${1:-$catalog}
    databricks schemas list "$catalog" --include-browse
}

