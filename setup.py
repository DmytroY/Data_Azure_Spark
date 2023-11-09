#!/usr/bin/env python

import setuptools

setuptools.setup(
    name='sparkbasics',
    version='1.0.0',
    description='BDCC Pyspark Basics project',
    packages=['src','src.jobs','src.shared'],
    include_package_data=True,
    install_requires=['pyspark', 'opencage', 'geohash2'],
    zip_safe=False
)
