#!/usr/bin/env python

from setuptools import setup

setup(
    name="tap-aconex",
    version="1.0.0",
    description="Singer.io tap for extracting data from the Aconex API",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_aconex"],
    install_requires=[
        "pipelinewise-singer-python==1.*",
        "requests==2.25.1",
        "xmltodict==0.12.0",
    ],
    extras_require={
        "dev": [
            "pylint",
            "ipdb",
            "nose",
        ]
    },
    entry_points="""
          [console_scripts]
          tap-aconex=tap_aconex:main
      """,
    packages=["tap_aconex"],
    package_data={"tap_aconex": ["tap_aconex/schemas/*.json"]},
    include_package_data=True,
)
