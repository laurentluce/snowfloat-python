#!/usr/bin/env python
"""
SnowFloat
=========

Python client for the SnowFloat API.
"""
import os
from setuptools import setup

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "snowfloat",
    version = "0.10",
    author = "SnowFloat",
    author_email = "snowfloat@snowfloat.com",
    description = ('Client for the SnowFloat geo API (https://www.snowfloat.com)'),
    license = "MIT",
    keywords = "Geo data management computation cloud",
    url = 'https://www.snowfloat.com',
    packages=['snowfloat'],
    long_description=read('README'),
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Topic :: Utilities",
        "License :: OSI Approved :: MIT License",
    ],
)
