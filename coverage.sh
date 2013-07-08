#!/bin/bash

coverage run --source=snowfloat/,tests.py tests.py
coverage report -m
coverage html
