#!/bin/bash

coverage run --source=snowfloat,tests -m unittest discover
coverage report -m
coverage html
