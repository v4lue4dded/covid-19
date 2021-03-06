#!/bin/bash

printf "\n\n\nstart of run at `date +'%Y-%m-%d %H:%M:%S'`\n"
cd ~/../../data/repos/covid-19/


git status
git pull
git submodule update --remote
git status
/data/tools/anaconda3/bin/python merge_tables.py
git status
git add date.txt
git add COVID-19
git add covid-19-data
git add df_data_clean.tsv
git add df_data_clean_max_date.tsv
git add df_lu_clean.tsv
git status
git commit -m "date update: `date +'%Y-%m-%d %H:%M:%S'`";
git status
git push origin master
git status
git lfs prune
