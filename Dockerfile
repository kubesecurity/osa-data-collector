FROM registry.access.redhat.com/ubi8/ubi-minimal

LABEL name="osa data collector" \
      description="Worker that collects data from github repo based by using bigquery" \
      email-ids="rzalavad@redhat.com" \
      git-url="https://github.com/kubesecurity/osa-data-collector.git" \
      git-path="/" \
      target-file="Dockerfile" \
      app-license="GPL-3.0"

ADD ./requirements.txt /app/
ADD run_data_collector.py /app/
COPY src/ /app/src/

RUN microdnf install python3 && pip3 install --upgrade pip &&\
    pip3 install -r /app/requirements.txt && rm /app/requirements.txt

ADD scripts/entrypoint.sh /app/entrypoint.sh

RUN mkdir /app/gh_data && chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
