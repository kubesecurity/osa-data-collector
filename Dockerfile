FROM registry.access.redhat.com/ubi8/ubi-minimal

LABEL name="osa data collector" \
      description="Worker that collects data from github repo based by using bigquery" \
      email-ids="rzalavad@redhat.com" \
      git-url="https://github.com/kubesecurity/osa-data-collector.git" \
      git-path="/" \
      target-file="Dockerfile" \
      app-license="GPL-3.0"

ADD ./requirements.txt /
ADD run_data_collector.py /

RUN microdnf install git && microdnf install python3 && pip3 install --upgrade pip &&\
    pip3 install -r /requirements.txt && rm /requirements.txt

COPY src/ /src/

ADD scripts/entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
