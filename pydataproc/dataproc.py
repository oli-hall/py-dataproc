import time

import googleapiclient
from googleapiclient import discovery

from pydataproc.logger import log


class DataProc(object):
    """
    Wraps a DataProc client and region/project information, giving a
    single point to check cluster status, submit jobs, create/tear down
    clusters, etc.
    """

    def __init__(self, project, region='europe-west1', zone='europe-west1-b'):
        self.dataproc = self.get_client()
        self.project = project
        self.region = region
        self.zone = zone

    def get_client(self):
        """Builds a client to the dataproc API."""
        dataproc = googleapiclient.discovery.build('dataproc', 'v1')
        return dataproc

    def is_running(self, cluster_name):
        """
        Returns True if the cluster with the provided name is in
        the RUNNING state, or False otherwise.

        :param cluster_name: the cluster name to check
        :return: True if the cluster is RUNNING, False otherwise
        """
        state = self.cluster_state(cluster_name)
        return state and state == 'RUNNING'

    def cluster_state(self, cluster_name):
        """
        Returns the current state of the cluster with the name
        provided. If the cluster does not exist, None is returned.

        :param cluster_name: The name of the cluster to check
        :return: cluster state string, or None if no such cluster
        """
        clusters = self.list_clusters()
        for c in clusters['clusters']:
            if cluster_name == c['clusterName']:
                return c['status']['state']

        return None

    def cluster_bucket(self, cluster_name):
        """
        Returns the staging GCS bucket associated with the provided
        cluster name. If the cluster does not exist, None is returned.

        :param cluster_name: The name of the cluster to check
        :return: staging bucket, or None if no such cluster
        """
        clusters = self.list_clusters()
        for c in clusters['clusters']:
            if cluster_name == c['clusterName']:
                return c['config']['configBucket']

        return None

    def list_clusters(self):
        """
        Queries the DataProc API, returning a list of all currently active clusters,
        with all available details/configuration.

        :return: list of dicts of cluster configuration
        """
        result = self.dataproc.projects().regions().clusters().list(
            projectId=self.project,
            region=self.region).execute()
        return result

    # TODO add support for preemptible workers
    def create_cluster(self, cluster_name, num_masters=1, num_workers=2,
                       master_type='n1-standard-1', worker_type='n1-standard-1',
                       master_disk_gb=50, worker_disk_gb=50):
        """Creates a DataProc cluster with the provided settings, returning a dict
        of the results returned from the API

        :param cluster_name: the name of the cluster
        :param num_masters: the number of master instances to use (default: 1)
        :param num_workers: the number of worker instances to use (default: 2)
        :param master_disk_gb: the size of the boot disk on each master (default: 50GB)
        :param worker_disk_gb: the size of the boot disk on each worker (default: 50GB)
        :param master_type: the type of instance to use for each master (default: n1-standard-1)
        :param worker_type: the type of instance to use for each worker (default: n1-standard-1)
        """
        log.info('Creating cluster {}...'.format(cluster_name))
        zone_uri = 'https://www.googleapis.com/compute/v1/projects/{}/zones/{}'.format(
                self.project, self.zone)

        cluster_data = {
            'projectId': self.project,
            'clusterName': cluster_name,
            'config': {
                'gceClusterConfig': {
                    'zoneUri': zone_uri
                },
                'workerConfig': {
                    'numInstances': num_workers,
                    'machineTypeUri': worker_type,
                    'diskConfig': {
                        'bootDiskSizeGb': worker_disk_gb
                    }
                },
                'masterConfig': {
                    'numInstances': num_masters,
                    'machineTypeUri': master_type,
                    'diskConfig': {
                        'bootDiskSizeGb': master_disk_gb
                    }
                },
                'initializationActions': [
                    {
                        'executableFile': 'gs://omicron-staging-test/cluster/dataproc-py3.sh'
                    }
                ]
            }
        }
        log.debug('Cluster settings: {}'.format(cluster_data))
        result = self.dataproc.projects().regions().clusters().create(
            projectId=self.project,
            region=self.region,
            body=cluster_data).execute()
        log.info('Cluster {} created'.format(cluster_name))
        return result

    def delete_cluster(self, cluster_name):
        """
        Deletes the cluster with the provided name, if it exists.

        :param cluster_name: the name of the cluster to delete
        :return: the (dict) results of the deletion
        """
        log.info('Tearing down cluster {}...'.format(cluster_name))
        result = self.dataproc.projects().regions().clusters().delete(
            projectId=self.project,
            region=self.region,
            clusterName=cluster_name).execute()
        return result

    def submit_job(self, job_details):
        """
        Submit a job to a cluster.

        :param job_details: A dict describing the job to be submitted
        :return: the results of the job submission call
        """
        result = self.dataproc.projects().regions().jobs().submit(
            projectId=self.project,
            region=self.region,
            body=job_details).execute()
        return result

    def wait_for_job(self, job_id):
        """
        A blocking call that, given a job ID, waits for the job to reach a
        finished state.

        :param job_id: The ID of the job to wait for
        :return: the results of the job, once complete
        """
        log.info("Waiting for job {} to finish...".format(job_id))
        while True:
            result = self.dataproc.projects().regions().jobs().get(
                projectId=self.project,
                region=self.region,
                jobId=job_id).execute()
            if result['status']['state'] == 'ERROR':
                log.info('Error running job: {}'.format(result['status']['details']))
                return result
            elif result['status']['state'] == 'DONE':
                log.info('Job finished.')
                return result
            log.debug("Job state: {}".format(result['status']['state']))
            time.sleep(5)
