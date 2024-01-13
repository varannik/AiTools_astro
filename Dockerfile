FROM quay.io/astronomer/astro-runtime:10.0.0

WORKDIR "/usr/local/airflow"
COPY dbt-requirements.txt ./
COPY scrapper-requirements.txt ./

# install dbt into a virtual environment

RUN python -m virtualenv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir -r dbt-requirements.txt && deactivate

# USER root
# RUN python -m virtualenv scrapper_venv && source scrapper_venv/bin/activate && \
#     apt-get update && apt-get -y install libpq-dev && \
#     pip install --no-cache-dir -r scrapper-requirements.txt && deactivate
