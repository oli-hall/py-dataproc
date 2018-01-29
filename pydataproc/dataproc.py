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
        """
        Allows the user to interact with a specific cluster or all
        clusters (depending upon whether cluster_name is specified).

        If cluster_name is specified, but there is no cluster with
        that name, raises a NoSuchClusterException

        :param cluster_name: string, name of cluster to fetch (optional)
        :return: Cluster/Clusters
        """
        if cluster_name:
            return Cluster(self, cluster_name)

        return Clusters(self)

    def jobs(self, job_id=None):
        """
        Allows the user to interact with a specific job or all
        jobs (depending upon whether job_id is specified).

        If job_id is specified, but there is no cluster with
        that name, raises a NoSuchJobException

        :param job_id: string, ID of job to fetch (optional)
        :return: Job/Jobs
        """
        if job_id:
            return Job(self, job_id)

        return Jobs(self)
