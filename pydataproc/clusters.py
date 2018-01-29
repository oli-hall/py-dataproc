import time

from googleapiclient.errors import HttpError

from cluster import Cluster
from logger import log
from errors import ClusterAlreadyExistsException

class Clusters(object):

    def __init__(self, dataproc):

        assert dataproc

        self.dataproc = dataproc

    def list(self, minimal=True):
        """
        Queries the DataProc API, returning a dict of all currently active clusters,
        keyed by cluster name.

        If 'minimal' is specified, each cluster's current state will be returned,
        otherwise the full cluster configuration will be returned.

        :param minimal: returns only the cluster state if set to True.
        :return: dict of cluster name -> cluster information
        """
        result = self.dataproc.client.projects().regions().clusters().list(
            projectId=self.dataproc.project,
            region=self.dataproc.region).execute()
        if minimal:
            return {c['clusterName']: c['status']['state'] for c in result.get('clusters', [])}
        return {c['clusterName']: c for c in result.get('clusters', [])}


    # TODO add support for preemptible workers
    def create(self, cluster_name, num_masters=1, num_workers=2,
               master_type='n1-standard-1', worker_type='n1-standard-1',
               master_disk_gb=50, worker_disk_gb=50, init_scripts=[], block=True):
        """Creates a DataProc cluster with the provided settings, returning a dict
        of the results returned from the API. It can wait for cluster creation if desired.

        If block is set to True, the method will block until the cluster reaches either
        a RUNNING or an ERROR state. If the cluster errors, an Exception will be raised.

        :param cluster_name: the name of the cluster
        :param num_masters: the number of master instances to use (default: 1)
        :param num_workers: the number of worker instances to use (default: 2)
        :param master_disk_gb: the size of the boot disk on each master (default: 50GB)
        :param worker_disk_gb: the size of the boot disk on each worker (default: 50GB)
        :param master_type: the type of instance to use for each master (default: n1-standard-1)
        :param worker_type: the type of instance to use for each worker (default: n1-standard-1)
        :param init_scripts: location initialisation scripts (default: [])
        :param block: whether to block upon cluster creation.

        :return: Cluster object
        """
        log.info("Creating cluster '{}'".format(cluster_name))
        zone_uri = 'https://www.googleapis.com/compute/v1/projects/{}/zones/{}'.format(
                self.dataproc.project, self.dataproc.zone)

        cluster_data = {
            'projectId': self.dataproc.project,
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
                }
            }
        }

        if init_scripts:
            cluster_data['config']['initializationActions'] = [
                {'executableFile': init_script} for init_script in init_scripts
            ]

        log.debug('Cluster settings: {}'.format(cluster_data))

        try:
            result = self.dataproc.client.projects().regions().clusters().create(
                projectId=self.dataproc.project,
                region=self.dataproc.region,
                body=cluster_data
            ).execute()
        except HttpError as e:
            if e.resp['status'] == '409':
                raise ClusterAlreadyExistsException("Cluster '{}' already exists".format(cluster_name))
            raise e

        log.debug("Create call for cluster '{}' returned: {}".format(cluster_name, result))

        cluster = Cluster(self.dataproc, cluster_name)

        if not block:
            return cluster


        status = cluster.status()
        log.info("Waiting for cluster to be ready...")
        while not status in ['RUNNING', 'ERROR']:
            time.sleep(5)
            status = cluster.status()

        if status == 'ERROR':
            cluster_info = cluster.info()
            status_detail = cluster_info['status'].get('detail', '')
            raise Exception("Cluster encountered an error: {}".format(status_detail))

        log.info("Cluster '{}' is ready.".format(cluster_name))
        return cluster
