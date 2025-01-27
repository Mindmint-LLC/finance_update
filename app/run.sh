#!/bin/bash

python load_netsuite.py

cd dbt_finance
dbt build

echo "Sending GL to GSheet"
cd ..
python fct_gl_summary.py

# for i in {1..4}; do echo Processing $i; sleep 1; done
echo "All done!"
