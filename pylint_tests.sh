#!/bin/bash

# W0212: access to private methods.
pylint -i y --disable=R0904,W0212 tests
