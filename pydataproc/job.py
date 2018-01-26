import subprocess

from googleapiclient.errors import HttpError

from logger import log
from errors import NoSuchJobException


class Job(object):

    def __init__(self, dataproc, job_id):

        assert dataproc
        assert job_id

        self.dataproc = dataproc
        self.job_id = job_id

    def info(self):
        """
        Returns the full configuration information associated with a given
        job. If the job does not exist, returns None.

        :return: dict of job information, or None if no such cluster
        """
        try:
            return self.dataproc.client.projects().regions().jobs().get(
                projectId=self.dataproc.project,
                region=self.dataproc.region,
                jobId=self.job_id
            ).execute()
        except HttpError as e:
            if e.resp['status'] == '404':
                raise NoSuchJobException("No job found with ID {}".format(self.job_id))
            raise e

    def wait(self, stream_logs=True):
        """
        A blocking call that waits for the job to reach a finished state.
        By default streams job logs to stdout, using the 'gcloud client jobs wait'
        command and subprocess.

        :return: string, status of the job once complete
        """
        log.info("Waiting for job {} to finish...".format(self.job_id))
        self._stream_logs(stream_logs)

        status = self.status()
        if status == 'ERROR':
            log.info('Error running job: {}'.format(self.info()['status']['details']))
        elif status == 'DONE':
            log.info('Job finished.')
        log.debug("Job status: {}".format(status))
        return status

    def _stream_logs(self, stream_logs=False):
        """
        Streams the job logs to stdout, using the 'gcloud client jobs wait'
        command and subprocess.

        :return: None
        """
        # piping stdout to PIPE ensures that the job configuration isn't output
        # when the job completes
        # This isn't yet supported by the DataProc API/Python lib, so must be done
        # using subprocess and the gcloud CLI tools
        if stream_logs:
            print('\nJOB LOGS (job ID: {}):\n--------------------------\n'.format(self.job_id))
            subprocess.call(
                "gcloud dataproc jobs wait --region {} {}".format(self.dataproc.region, self.job_id).split(),
                stdout=subprocess.PIPE
            )
            print('\n--------------------------\n')
        else:
            # piping stderr to PIPE suppresses all output
            subprocess.call(
                "gcloud dataproc jobs wait --region {} {}".format(self.dataproc.region, self.job_id).split(),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )

    def exists(self):
        """
        Checks if the job exists.

        :return: boolean, True if job exists, False otherwise.
        """
        try:
            self.dataproc.client.projects().regions().jobs().get(
                projectId=self.dataproc.project,
                region=self.dataproc.region,
                jobId=self.job_id
            ).execute()
            return True
        except HttpError as e:
            if e.resp['status'] == '404':
                return False
            raise e

    def status(self):
        """
        Fetch status of job

        :return: string, job status
        """
        return self.info()['status']['state']

    # TODO delete/cancel/is_running/succeeded
