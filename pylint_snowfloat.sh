#!/bin/bash

# W0212: access to private methods.
pylint -i y --disable=R0903,R0904 --generated-members=sha256 snowfloat

