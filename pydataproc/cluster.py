from googleapiclient.errors import HttpError

from pydataproc.job import Job
from pydataproc.logger import log
from pydataproc.errors import NoSuchClusterException, ClusterHasGoneAwayException


class Cluster(object):
    """
    Class to wrap a selection of utility methods for querying/updating
    a DataProc cluster.

    Currently, this will raise an Exception if the cluster does not exist
    during any method call.
    """

    # TODO handle cluster deletion more gracefully
    def __init__(self, dataproc, cluster_name):

        assert dataproc
        assert cluster_name

        self.dataproc = dataproc
        self.cluster_name = cluster_name

        if not self.exists():
            raise NoSuchClusterException("Cluster '{}' does not exist".format(cluster_name))

    def exists(self):
        """
        Checks if the cluster exists.

        :return: boolean, True if cluster exists, False otherwise.
        """
        try:
            self.dataproc.client.projects().regions().clusters().get(
                projectId=self.dataproc.project,
                region=self.dataproc.region,
                clusterName=self.cluster_name
            ).execute()
            return True
        except HttpError as e:
            if e.resp['status'] == '404':
                return False
            raise e

    def is_running(self):
        """
        Returns True if the cluster is in the RUNNING state, or
        False otherwise.

        :return: True if the cluster is RUNNING, False otherwise
        """
        state = self.status()
        return state and state == 'RUNNING'


    def status(self):
        """
        Returns the current state of the cluster.

        :return: string, cluster state
        """
        info = self.info()
        return info['status']['state']

    def info(self):
        """
        Returns the full cluster information.

        :return: dict, cluster information
        """
        try:
            return self.dataproc.client.projects().regions().clusters().get(
                projectId=self.dataproc.project,
                region=self.dataproc.region,
                clusterName=self.cluster_name
            ).execute()
        except HttpError as e:
            if e.resp['status'] == '404':
                raise ClusterHasGoneAwayException("'{}' no longer exists".format(self.cluster_name))
            raise e


    def bucket(self):
        """
        Returns the staging GCS bucket associated with the cluster.

        :return: string, staging bucket
        """
        info = self.info()
        return info['config']['configBucket']

    # TODO allow changing of secondary worker count
    def change_worker_count(self, worker_count):
        """
        Update worker count for the cluster.

        :param worker_count: int, new worker count
        :return:
        """
        assert worker_count

        patch_config = {
            "config": {
                "workerConfig": {
                    "numInstances": worker_count
                }
            }
        }
        try:
            result = self.dataproc.client.projects().regions().clusters().patch(
                projectId=self.dataproc.project,
                region=self.dataproc.region,
                clusterName=self.cluster_name,
                updateMask='config.worker_config.num_instances',
                body=patch_config).execute()
        except HttpError as e:
            if e.resp['status'] == '404':
                raise ClusterHasGoneAwayException("'{}' no longer exists".format(self.cluster_name))
            raise e

        # TODO optionally wait for result
        if result['metadata']['@type'] != 'type.googleapis.com/google.cloud.client.v1.ClusterOperationMetadata':
            raise Exception('Failed to update worker count on cluster {}'.format(self.cluster_name))
        return result

    def delete(self):
        """
        Deletes the cluster.

        :return: the (dict) results of the deletion
        """
        # TODO this doesn't deal with clusters that aren't running (e.g. that failed on startup)
        log.info('Tearing down cluster {}...'.format(self.cluster_name))
        try:
            return self.dataproc.client.projects().regions().clusters().delete(
                projectId=self.dataproc.project,
                region=self.dataproc.region,
                clusterName=self.cluster_name).execute()
        except HttpError as e:
            if e.resp['status'] == '404':
                raise ClusterHasGoneAwayException("'{}' no longer exists".format(self.cluster_name))
            raise e

    def submit_job(self, file_to_run=None, python_files=None, args="", job_details=None):
        """
        Submit a PySpark job to the cluster. Allows optional specification of
        additional python files, and any job arguments required.

        :param file_to_run: The PySpark file to run. Must be a Google Storage path.
        :param python_files: Specify additional files or a zip location containing additional
        python files to pass to the job. Must be Google Storage paths. Defaults to None.
        :param args: job arguments, as a string. Specify args as you would on the command line, e.g.
        "-flag1 value_for_flag1 -flag2 value_for_flag2 unflagged_arg"
        :param job_details: the full job_details dict. If specified, overrides the other arguments,
        and is passed directly to the job submission call.
        :return: the results of the job submission call
        """
        job_details = job_details or self._build_job_details(file_to_run, python_files, args)

        try:
            result = self.dataproc.client.projects().regions().jobs().submit(
                projectId=self.dataproc.project,
                region=self.dataproc.region,
                body=job_details
            ).execute()

            return Job(self.dataproc, result['reference']['jobId'])
        except HttpError as e:
            if e.resp['status'] == '404':
                raise ClusterHasGoneAwayException("'{}' no longer exists".format(self.cluster_name))
            raise e

    def _build_job_details(self, file_to_run, python_files=None, args=""):
        if python_files:
            assert isinstance(python_files, list)

        assert file_to_run

        job_details = {
            "projectId": self.dataproc.project,
            "job": {
                "placement": {
                    "clusterName": self.cluster_name
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
