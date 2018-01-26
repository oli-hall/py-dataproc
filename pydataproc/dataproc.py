import googleapiclient
from googleapiclient import discovery

from cluster import Cluster
from clusters import Clusters
from job import Job
from jobs import Jobs


class DataProc(object):
    """
    Wraps a DataProc client and region/project information, giving a
    single point to interact with Clusters and Jobs.
    """

    def __init__(self, project, region='europe-west1', zone='europe-west1-b'):
        self.client = self._get_client()
        self.project = project
        self.region = region
        self.zone = zone

    def _get_client(self):
        """Builds a client to the DataProc API."""
        return googleapiclient.discovery.build('dataproc', 'v1', cache_discovery=False)

    def clusters(self):
        return Clusters(self)

    def cluster(self, cluster_name):
        return Cluster(self, cluster_name)

    def jobs(self):
        return Jobs(self)

    def job(self, job_id):
        return Job(self, job_id)
