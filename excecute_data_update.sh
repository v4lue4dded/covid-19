#!/bin/bash

git submodule foreach --recursive 'git pull'
python merge_tables.py
git add COVID-19
git add covid-19-data
git add df_data_clean.tsv
git add df_data_clean_max_date.tsv
git add df_lu_clean.tsv
git commit -m "date update: `date +'%Y-%m-%d %H:%M:%S'`";
git push origin master