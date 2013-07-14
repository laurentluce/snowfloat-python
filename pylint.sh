#!/bin/bash

pylint -i y --disable=R0903,R0904,W0223,E0611,F0401 --generated-members=sha256 snowfloat

# W0212: access to private methods.
pylint -i y --disable=R0904,W0212 tests
