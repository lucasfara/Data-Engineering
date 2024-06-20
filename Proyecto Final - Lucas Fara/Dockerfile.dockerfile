FROM apache/airflow:2.0.0

USER root
RUN apt-get update && apt-get install -y \
    python3-pip \
    && pip3 install pandas sqlalchemy requests psycopg2-binary

USER airflow
COPY dags /usr/local/airflow/dags
COPY scripts /usr/local/airflow/scripts

ENTRYPOINT ["/entrypoint"]
CMD ["webserver"]