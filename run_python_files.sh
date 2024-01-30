#!/bin/bash
cd ~
cd ./website_scrape/aldi
python3 main_v2.py  
cd ..

cd ./iga
python3 sale_id.py
python3 main_v2.py  
cd ..

cd ./coles
python3 update_api.py
python3 main_v2.py  
cd ..

cd ./woolworths
python3 main_v2.py  
cd ..