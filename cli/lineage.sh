# Only account admins can enable system schemas
# - azure: https://accounts.azuredatabricks.net/users
#
# enable system.access

METASTORE_ID=$(curl -s https://raw.githubusercontent.com/davidkhala/databricks-common/refs/heads/main/cli/current.sh | bash -s metastore)
databricks system-schemas enable $METASTORE_ID access

