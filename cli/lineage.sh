# The caller must be an account admin or a metastore admin.
# see in https://docs.databricks.com/api/workspace/systemschemas/enable
#


METASTORE_ID=$(curl -s https://raw.githubusercontent.com/davidkhala/databricks-common/refs/heads/main/cli/current.sh | bash -s metastore)
databricks system-schemas enable $METASTORE_ID access # enable system.access

