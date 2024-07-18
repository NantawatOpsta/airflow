FROM apache/airflow:2.9.3
ADD requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt

USER root
RUN adduser vscode
RUN groupadd docker
RUN usermod -aG docker vscode

USER airflow