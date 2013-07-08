#!/bin/bash

pylint -i y --disable=R0903,R0904,W0223,E0611,F0401 --generated-members=sha256 snowfloat tests.py
