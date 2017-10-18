### py-dataproc

A Python wrapper around the Google Dataproc client. 

Essentially, the Google Python client for Dataproc uses the generic `googleapiclient` interface, and is somewhat clunky to use. Until it's ported to the new `google.cloud` tooling, this class should make working with DataProc from Python a lot easier.

#### Setup

Clone the repo, create a virtualenv, install the requirements, and you're good to go!

```bash
virtualenv bin/env
source bin/env/bin/activate
pip install -r requirements.pip
```

I'll look into packaging this up as an installable lib next, but for now, it's a bit manual.

#### Requirements

This has been tested with Python 2.7, but should be Python 3 compatible. More thorough testing will follow.

### Usage

There are method level docs, and higher-level usage instructions will follow in a bit.
