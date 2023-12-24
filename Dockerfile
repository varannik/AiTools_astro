FROM quay.io/astronomer/astro-runtime:10.0.0

WORKDIR "/usr/local/airflow"
COPY dbt-requirements.txt ./
# install dbt into a virtual environment

RUN python -m virtualenv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir -r dbt-requirements.txt && deactivate