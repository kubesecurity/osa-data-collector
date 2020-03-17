#!/usr/bin/env bash
set +ex

ecosystem_list=("openshift" "knative" "kubevirt")

# Run the data collector for given eco systems and no of days to  collect data
python3 run_data_collector.py -e ${ecosystem_list[*]} -d ${DAYS};
