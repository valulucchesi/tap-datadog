#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-datadog",
    version="0.1.0",
    description="Singer.io tap for extracting Fastly billing data.",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap-datadog"],
    install_requires=[
        "singer-python>=5.0.12",
        "requests",
        "pendulum"
    ],
    entry_points="""
    [console_scripts]
    tap-datadog=tap_datadog:main
    """,
    packages=["tap_datadog"],
    package_data = {
        "schemas": ["tap_datadog/schemas/*.json"]
    },
    include_package_data=True,
)
