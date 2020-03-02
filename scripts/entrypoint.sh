#!/usr/bin/env bash
set +ex

ecosystem_list=("openshift" "knative" "kubevirt")

ls -ltr

ls -ltr app
# Run the inference for each ecosystem in the inference list.
# for ecosystem in ${ecosystem_list[*]};do
python3 app/run_data_collector.py -eco-systems ${ecosystem_list[*]} -days 1;
# done
