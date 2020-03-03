#!/usr/bin/env bash
set +ex

ecosystem_list=("openshift" "knative" "kubevirt")

# Run the data collector for given eco systems and no of days to colelct data
python3 app/run_data_collector.py -eco-systems ${ecosystem_list[*]} -days ${DAYS};
