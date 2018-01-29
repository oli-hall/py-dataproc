# py-dataproc

A Python wrapper around the Google Dataproc client. 

Essentially, the Google Python client for Dataproc uses the generic `googleapiclient` interface, and is somewhat clunky to use. Until it's ported to the new `google.cloud` tooling, this class should make working with DataProc from Python a lot easier.

## Setup

Clone the repo, create a virtualenv, install the requirements, and you're good to go!

```bash
virtualenv bin/env
source bin/env/bin/activate
python setup.py install
```

Setuptools should mean you can install directly from GitHub, by putting the following in your requirements file:

```bash
git+git://github.com/oli-hall/py-dataproc.git#egg=pydataproc
```

## Requirements

This has been tested with Python 2.7, but should be Python 3 compatible. More thorough testing will follow.

## Usage

N.B. You will need to auth your shell with Google Cloud before connecting. To do this (assuming you have `gcloud` CLI tools installed), run the following:

```gcloud auth login```

This will pop up an authentication dialogue in your browser of choice, and once you accept, it will authorise your shell with Google Cloud.

To initialise a client, provide the `DataProc` class with your Google Cloud Platform project ID, and optionally the region/zone you wish to connect to (defaults to `europe-west1`).

```python
> dataproc = DataProc("gcp-project-id", region="europe-west1", zone="europe-west1-b")
```

### Versions

The API has been updated significantly as of version 0.7.0. The documentation below includes the newer API first, but still gives a brief overview of the previous API for completeness.

#### Latest API

##### Working with clusters

Clusters are accessed through a single interface: `clusters`. This will give you access to a single cluster, by providing the `cluster_name` parameter, or all clusters.

To list all clusters:

```python
> dataproc.clusters().list()
```

By default, this will return a minimal dictionary of cluster name -> cluster state. There is a boolean `minimal` flag which, if set to false, will give all the current cluster information returned by the underlying API, again keyed by cluster name. 


##### Creating a cluster

Creating a cluster is where the DataProc client really becomes useful. Rather than specifying a giant dictionary, simply call `create`. This will allow configuration of machine types, number of instances, etc, but abstracts away the API details:

```python
> dataproc.clusters().create(
    cluster_name="my-cluster",
    num_workers=4,
    worker_disk_gb=250
)
``` 

It also allows blocking for the cluster to be created. This method will return a `Cluster` object, which we'll dive into next.


##### Individual Clusters

Individual clusters can be queried and updated, through the `Cluster` class. You can create one of these either by calling `clusters().create()` or by calling `clusters()` with a cluster name parameter:

```python
> dataproc.clusters("my-cluster")
```

N.B. If you call `dataproc.clusters()` with a non-existent cluster name, an Exception will be raised.

To fetch cluster information for a particular cluster, fetch the cluster, then call `info`:

```python
> dataproc.clusters("my-cluster").info()
```

And similarly to fetch the status:

```python
> dataproc.clusters("my-cluster").status()
```

This returns just the status of the cluster requested as a string. If you're just interested in knowing if it's running...

```python
> dataproc.clusters("my-cluster").is_running()
```

Deleting a cluster:

```python
> dataproc.clusters("my-cluster").delete()
```

##### Working with jobs

Submitting a job can be as simple or as complex as you want. You can simply specify a cluster, the Google Storage location of a pySpark job, and job args...

```python
> dataproc.clusters("my-cluster").submit_job("gs://my_bucket/jobs/my_spark_job.py", args="-arg1 some_value -arg2 something_else")
```

...or you can submit the full job details dictionary yourself (see the [DataProc API docs](https://cloud.google.com/dataproc/docs/reference/rest/) for more information).

#### Previous API - versions 0.6.2 and below

##### Working with existing clusters

There are a few different commands to work with existing clusters. You can list all clusters, get the state of a particular cluster, or return various pieces of information about said cluster.

To list all clusters:

```python
> dataproc.list_clusters()
```

By default, this will return a minimal dictionary of cluster name -> cluster state. There is a boolean `minimal` flag which, if set to false, will give all the current cluster information returned by the underlying API, again keyed by cluster name. 

To fetch cluster information for a particular cluster:

```python
> dataproc.cluster_info("my-cluster")
```

And similarly to fetch the state:

```python
> dataproc.cluster_state("my-cluster")
```

This returns just the state of the cluster requested as a string. If you're just interested in knowing if it's running...

```python
> dataproc.is_running("my-cluster")
```

##### Creating a cluster

Creating a cluster is where the DataProc client really becomes useful. Rather than specifying a giant dictionary, simply call `create_cluster`. This will allow configuration of machine types, number of instances, etc, but abstracts away the API details:

```python
> dataproc.create_cluster(
    cluster_name="my-cluster",
    num_workers=4,
    worker_disk_gb=250
)
``` 

It also allows blocking for the cluster to be created (N.B. currently this will block forever if the cluster fails to initialise correctly, so be careful!).

##### Deleting a cluster

```python
> dataproc.delete_cluster("my-cluster")
```

##### Working with jobs

Submitting a job can be as simple or as complex as you want. You can simply specify a cluster, the Google Storage location of a pySpark job, and job args...

```python
> dataproc.submit_job("my-cluster", "gs://my_bucket/jobs/my_spark_job.py", args="-arg1 some_value -arg2 something_else")
```

...or you can submit the full job details dictionary yourself (see the [DataProc API docs](https://cloud.google.com/dataproc/docs/reference/rest/) for more information).
