from setuptools import setup

install_requires = [
    'google-api-python-client==1.6.3',
]

test_requires = [
]

setup(
    name='pydataproc',
    version='0.7.1',
    author='Oli Hall',
    author_email='',
    description="Python wrapper for the Google DataProc client",
    license='MIT',
    url='https://github.com/oli-hall/py-dataproc',
    packages=['pydataproc'],
    setup_requires=[],
    install_requires=install_requires,
    tests_require=test_requires,
)
