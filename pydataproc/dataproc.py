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

    def clusters(self, cluster_name=None):
        if cluster_name:
            cluster = Cluster(self, cluster_name)
            if not cluster.exists():
                raise Exception("Cluster '{}' does not exist".format(cluster_name))
            return cluster

        return Clusters(self)

    def jobs(self, job_id=None):
        if job_id:
            job = Job(self, job_id)
            if not job.exists():
                raise Exception("Job '{}' does not exist".format(job_id))
            return job

        return Jobs(self)
