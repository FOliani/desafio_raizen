FROM apache/airflow:2.3.0
USER root

#updating system and installing necessary features
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         libreoffice \
         openjdk-11-jdk \
         ant \ 
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

#installing necessary libraries
RUN pip install openpyxl 
RUN pip install fastparquet
