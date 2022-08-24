"""
This file configures the Python package with entrypoints used for future runs on Databricks.

Please follow the `entry_points` documentation for more details on how to configure the entrypoint:
* https://setuptools.pypa.io/en/latest/userguide/entry_point.html
"""

from setuptools import find_packages, setup
from test_project import __version__

setup(
    name="test_project",
    packages=find_packages(exclude=["tests", "tests.*"]),
    setup_requires=["wheel"],
    entry_points = {
        "console_scripts": [
            "etl = test_project.workloads.sample_etl_job:entrypoint",
            "ml = test_project.workloads.sample_ml_job:entrypoint",
            "dummy_job = test_project.workloads.my_dummy_job:entrypoint"
    ]},
    version=__version__,
    description="",
    author="",
)
