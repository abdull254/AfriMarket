FROM apache/spark:3.5.0

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl bzip2 procps && \
    rm -rf /var/lib/apt/lists/*

RUN curl -fL \
    https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-$(uname -m).sh \
    -o /tmp/miniforge.sh && \
    bash /tmp/miniforge.sh -b -p /opt/conda && \
    rm /tmp/miniforge.sh && \
    /opt/conda/bin/conda install -y python=3.11 pip && \
    /opt/conda/bin/conda clean -afy

ENV PATH="/opt/conda/bin:${PATH}"

# numpy and pandas are needed by pyspark.ml; pymysql writes scores to MySQL;
# redis reads and updates the live customer features.
RUN pip install --no-cache-dir pyspark==3.5.0 numpy==1.26.4 pandas==2.2.2 \
    pyarrow==15.0.2 pymysql==1.1.1 redis==5.2.1

ENV PYSPARK_PYTHON=/opt/conda/bin/python
ENV PYSPARK_DRIVER_PYTHON=/opt/conda/bin/python

WORKDIR /app

CMD ["bash"]
