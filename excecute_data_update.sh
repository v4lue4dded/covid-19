#!/bin/bash

git submodule foreach --recursive 'git pull'
python merge_tables.py