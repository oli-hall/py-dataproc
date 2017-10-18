### py-dataproc

A Python wrapper around the Google Dataproc client. 

Essentially, the Google Python client for Dataproc uses the generic `googleapiclient` interface, and is somewhat clunky to use. Until it's ported to the new `google.cloud` tooling, this class should make working with DataProc from Python a lot easier.

#### Setup

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

#### Requirements

This has been tested with Python 2.7, but should be Python 3 compatible. More thorough testing will follow.

### Usage

There are method level docs, and higher-level usage instructions will follow in a bit.
