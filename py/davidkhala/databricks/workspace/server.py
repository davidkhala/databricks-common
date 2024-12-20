from databricks.sdk import WorkspaceClient


class Cluster:
    def __init__(self, client: WorkspaceClient, cluster_id):
        self.client, self.cluster_id = client, cluster_id,

    def start(self):
        self.client.clusters.ensure_cluster_is_running(self.cluster_id)

    def stop(self):
        self.client.clusters.delete_and_wait(self.cluster_id)
