import time

import googleapiclient
from googleapiclient import discovery
from googleapiclient.errors import HttpError

from pydataproc.logger import log


class DataProc(object):
    """
    Wraps a DataProc client and region/project information, giving a
    single point to check cluster status, submit jobs, create/tear down
    clusters, etc.
    """

    MAX_JOBS = 500

    def __init__(self, project, region='europe-west1', zone='europe-west1-b'):
        self.dataproc = self.get_client()
        self.project = project
        self.region = region
        self.zone = zone

    def get_client(self):
        """Builds a client to the dataproc API."""
        dataproc = googleapiclient.discovery.build('dataproc', 'v1', cache_discovery=False)
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
        for c, st in self.list_clusters().items():
            if c == cluster_name:
                return st

        return None

    def cluster_info(self, cluster_name):
        """
        Returns the full cluster information associated with a given
        cluster. If the cluster does not exist, returns None.

        :param cluster_name: The name of the cluster to check
        :return: dict of cluster information, or None if no such cluster
        """
        for c, ci in self.list_clusters(minimal=False).items():
            if c == cluster_name:
                return ci

        return None

    def cluster_bucket(self, cluster_name):
        """
        Returns the staging GCS bucket associated with the provided
        cluster name. If the cluster does not exist, None is returned.

        :param cluster_name: The name of the cluster to check
        :return: staging bucket, or None if no such cluster
        """
        for c, ci in self.list_clusters(minimal=False):
            if c == cluster_name:
                return ci['config']['configBucket']

        return None

    def list_clusters(self, minimal=True):
        """
        Queries the DataProc API, returning a dict of all currently active clusters,
        keyed by cluster name.

        If 'minimal' is specified, each cluster's current state will be returned,
        otherwise the full cluster configuration will be returned.

        :param minimal: returns only the cluster state if set to True.
        :return: dict of cluster name -> cluster information
        """
        result = self.dataproc.projects().regions().clusters().list(
            projectId=self.project,
            region=self.region).execute()
        if minimal:
            return {c['clusterName']: c['status']['state'] for c in result.get('clusters', [])}
        return {c['clusterName']: c for c in result.get('clusters', [])}

    def list_jobs(self, minimal=True, running=True, count=10, cluster_name=None):
        """
        Queries the DataProc API, returning a dict of jobs, keyed by job ID.

        If 'minimal' is specified, each job's current state will be returned,
        otherwise the full job configuration will be returned.

        If cluster_name is specified and not None, the list of jobs will be filtered
        to only those that ran on this cluster.

        :param minimal: returns only the job state if set to True.
        :param running: returns only ACTIVE jobs if set to True.
        :param count: maximum number of jobs to return.
        :param cluster_name: filter by cluster name. Defaults to None.
        :return: dict of job ID -> job information
        """
        filter = None
        if running:
            filter = 'status.state = ACTIVE'

        cluster_name_filter = None
        if cluster_name:
            cluster_name_filter = cluster_name

        if count > self.MAX_JOBS:
            log.info('count of {} too high, limiting to {} jobs...'.format(count, self.MAX_JOBS))
            count = self.MAX_JOBS

        try:
            result = self.dataproc.projects().regions().jobs().list(
                projectId=self.project,
                region=self.region,
                filter=filter,
                clusterName=cluster_name_filter
            ).execute()
        except HttpError as e:
            if e.resp['status'] == '404':
                raise Exception("'{}' is not a valid cluster".format(cluster_name))
            raise e

        if 'jobs' not in result:
            return {}

        # TODO investigate using generators here
        page_token = result['nextPageToken']
        while len(result['jobs']) < count:
            moar = self.dataproc.projects().regions().jobs().list(
                projectId=self.project,
                region=self.region,
                filter=filter,
                clusterName=cluster_name_filter,
                pageToken=page_token
            ).execute()
            if len(moar['jobs']) == 0:
                break
            result['jobs'].extend(moar['jobs'])
            if 'nextPageToken' not in page_token:
                break
            page_token = moar['nextPageToken']

        result['jobs'] = result['jobs'][:count]

        if minimal:
            return {j['reference']['jobId']: j['status']['state'] for j in result.get('jobs', [])}
        return {j['reference']['jobId']: j for j in result.get('jobs', [])}

    def job_info(self, job_id):
        """
        Returns the full configuration information associated with a given
        job. If the job does not exist, returns None.

        :param job_id: The ID of the job to check
        :return: dict of job information, or None if no such cluster
        """
        assert job_id

        try:
            return self.dataproc.projects().regions().jobs().get(
                projectId=self.project,
                region=self.region,
                jobId=job_id
            ).execute()
        except HttpError as e:
            if e.resp['status'] == '404':
                return None
            raise e

    # TODO logs for specific job (optionally streaming logs)


    # TODO add support for preemptible workers
    def create_cluster(self, cluster_name, num_masters=1, num_workers=2,
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
                }
            }
        }

        if init_scripts:
            cluster_data['config']['initializationActions'] = [
                {'executableFile': init_script} for init_script in init_scripts
            ]

        log.debug('Cluster settings: {}'.format(cluster_data))
        result = self.dataproc.projects().regions().clusters().create(
            projectId=self.project,
            region=self.region,
            body=cluster_data).execute()
        log.info('Cluster {} created'.format(cluster_name))

        if not block:
            return result


        state = self.cluster_state(cluster_name)
        log.info("Waiting for cluster to be ready...")
        while not state in ['RUNNING', 'ERROR']:
            time.sleep(5)
            state = self.cluster_state(cluster_name)

        if state == 'ERROR':
            cluster_info = self.cluster_info(cluster_name)
            status_detail = cluster_info['status'].get('detail', '')
            raise Exception("Cluster encountered an error: {}".format(status_detail))

        return self.cluster_info(cluster_name)

    # TODO allow changing of secondary worker count
    def change_worker_count(self, cluster_name, worker_count):
        """
        Update worker count for a given cluster

        :param cluster_name: str, name of cluster to update
        :param worker_count: int, new worker count
        :return:
        """
        assert cluster_name
        assert worker_count

        if not self.is_running(cluster_name):
            raise Exception("No cluster found with name {}".format(cluster_name))

        patch_config = {
            "config": {
                "workerConfig": {
                    "numInstances": worker_count
                }
            }
        }

        result = self.dataproc.projects().regions().clusters().patch(
            projectId=self.project,
            region=self.region,
            clusterName=cluster_name,
            updateMask='config.worker_config.num_instances',
            body=patch_config).execute()

        # TODO optionally wait for result
        if result['metadata']['@type'] != 'type.googleapis.com/google.cloud.dataproc.v1.ClusterOperationMetadata':
            raise Exception('Failed to update worker count on cluster {}'.format(cluster_name))
        return result

    def delete_cluster(self, cluster_name):
        """
        Deletes the cluster with the provided name, if it exists.

        :param cluster_name: the name of the cluster to delete
        :return: the (dict) results of the deletion
        """
        # TODO this doesn't deal with clusters that aren't running (e.g. that failed on startup)
        log.info('Tearing down cluster {}...'.format(cluster_name))
        result = self.dataproc.projects().regions().clusters().delete(
            projectId=self.project,
            region=self.region,
            clusterName=cluster_name).execute()
        return result

    def submit_job(self, cluster_name=None, file_to_run=None, python_files=None, args="", job_details=None):
        """
        Submit a PySpark job to a cluster. Allows optional specification of
        additional python files, and any job arguments required.

        :param cluster_name: The name of the cluster to submit the job to
        :param file_to_run: The PySpark file to run. Must be a Google Storage path.
        :param python_files: Specify additional files or a zip location containing additional
        python files to pass to the job. Must be Google Storage paths. Defaults to None.
        :param args: job arguments, as a string. Specify args as you would on the command line, e.g.
        "-flag1 value_for_flag1 -flag2 value_for_flag2 unflagged_arg"
        :param job_details: the full job_details dict. If specified, overrides the other arguments,
        and is passed directly to the job submission call.
        :return: the results of the job submission call
        """
        job_details = job_details or self._build_job_details(cluster_name, file_to_run, python_files, args)

        result = self.dataproc.projects().regions().jobs().submit(
            projectId=self.project,
            region=self.region,
            body=job_details).execute()
        return result

    def _build_job_details(self, cluster_name, file_to_run, python_files=None, args=""):
        if python_files:
            assert isinstance(python_files, list)

        assert cluster_name
        assert file_to_run

        job_details = {
            "projectId": self.project,
            "job": {
                "placement": {
                    "clusterName": cluster_name
                },
                "pysparkJob": {
                    "mainPythonFileUri": file_to_run,
                    "args": args.split()
                },
            }
        }

        if python_files:
            job_details["job"]["pysparkJob"]["pythonFileUris"] = python_files

        return job_details

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
