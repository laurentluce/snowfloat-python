#!/bin/bash

python tests.py
if [ $? -eq 1 ]
then
    echo "Unit tests failed."
    exit 1
fi

bash coverage.sh | grep TOTAL | grep "100%"
if [ $? -eq 1 ]
then
    echo "Unit tests coverage less than 100%."
    exit 1
fi

bash pylint.sh
if [ $? -eq 1 ]
then
    echo "Pylint not 10/10."
    exit 1
fi

echo "OK"
