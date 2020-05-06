#!/bin/bash

sudo git submodule foreach --recursive 'git pull'
sudo python merge_tables.py
sudo git add COVID-19
sudo git add covid-19-data
sudo git add df_data_clean.tsv
sudo git add df_data_clean_max_date.tsv
sudo git add df_lu_clean.tsv
sudo git commit -m "date update: `date +'%Y-%m-%d %H:%M:%S'`";
sudo git push origin master