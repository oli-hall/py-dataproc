from googleapiclient.errors import HttpError

from logger import log


class Jobs(object):

    MAX_JOBS = 500

    def __init__(self, dataproc):

        assert dataproc

        self.dataproc = dataproc

    def list(self, minimal=True, running=True, count=10, cluster_name=None):
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
            result = self.dataproc.client.projects().regions().jobs().list(
                projectId=self.dataproc.project,
                region=self.dataproc.region,
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
            moar = self.dataproc.client.projects().regions().jobs().list(
                projectId=self.dataproc.project,
                region=self.dataproc.region,
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
