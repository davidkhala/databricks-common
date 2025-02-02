import warnings

from davidkhala.databricks.workspace import Workspace


class Cluster:
    cluster_id: str

    def __init__(self, w: Workspace):
        self.w = w

    def get_one(self) -> str | None:
        for cluster_id in self.w.cluster_ids():
            self.cluster_id = cluster_id
            return cluster_id
        return None

    def start(self):
        self.w.client.clusters.ensure_cluster_is_running(self.cluster_id)

    def stop(self):
        self.w.client.clusters.delete_and_wait(self.cluster_id)

    def pollute(self):
        warnings.warn(f"workspace.config.cluster_id changes {self.w.config.cluster_id}->{self.cluster_id}")
        self.w.config.cluster_id = self.cluster_id
