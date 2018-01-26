import subprocess
import time

from googleapiclient.errors import HttpError

from logger import log


class Job(object):

    # TODO naming of dataproc
    def __init__(self, dataproc, job_id):

        assert dataproc
        assert job_id

        self.dataproc = dataproc
        self.job_id = job_id

    # TODO roll this into the 'wait' command
    def stream_logs(self):
        """
        Streams the job logs to stdout, using the 'gcloud client jobs wait'
        command and subprocess.

        :return: None
        """
        print('\nJOB LOGS (job ID: {}):\n--------------------------\n'.format(self.job_id))
        # piping stdout to PIPE ensures that the job configuration isn't output
        # when the job completes
        # This isn't yet supported by the DataProc API/Python lib, so must be done
        # using subprocess and the gcloud CLI tools
        subprocess.call(
            "gcloud client jobs wait --region {} {}".format(self.dataproc.region, self.job_id).split(),
            stdout=subprocess.PIPE
        )
        print('\n--------------------------\n')

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
                return None
            raise e

    def wait(self):
        """
        A blocking call that waits for the job to reach a finished state.

        :return: the results of the job, once complete
        """
        log.info("Waiting for job {} to finish...".format(self.job_id))
        while True:
            result = self.dataproc.client.projects().regions().jobs().get(
                projectId=self.dataproc.project,
                region=self.dataproc.region,
                jobId=self.job_id
            ).execute()
            if result['status']['state'] == 'ERROR':
                log.info('Error running job: {}'.format(result['status']['details']))
                return result
            elif result['status']['state'] == 'DONE':
                log.info('Job finished.')
                return result
            log.debug("Job state: {}".format(result['status']['state']))
            time.sleep(5)

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
        info = self.info()
        return info['status']['state']

    # TODO delete/cancel/is_running/succeeded
