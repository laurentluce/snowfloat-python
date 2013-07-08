#!/bin/bash

python tests.py
if [ $? -eq 1 ]
then
    echo "Unit tests failed."
    exit 1
fi

sh coverage.sh | grep TOTAL | grep "100%"
if [ $? -eq 1 ]
then
    echo "Unit tests coverage less than 100%."
    exit 1
fi

pylint -i y --disable=R0903,R0904,W0223,E0611,F0401 --generated-members=sha256 snowfloat
if [ $? -eq 1 ]
then
    echo "Pylint not 10/10."
    exit 1
fi

echo "OK"
