FROM apache/airflow:3.0.0-python3.11
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get install -y default-jdk \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"


USER airflow



RUN python3 --version
RUN pip --version
RUN pip cache purge
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt