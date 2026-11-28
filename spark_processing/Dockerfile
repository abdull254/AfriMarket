FROM apache/spark:3.5.0

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl bzip2 procps && \
    rm -rf /var/lib/apt/lists/*

RUN curl -fL \
    https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-x86_64.sh \
    -o /tmp/miniforge.sh && \
    bash /tmp/miniforge.sh -b -p /opt/conda && \
    rm /tmp/miniforge.sh && \
    /opt/conda/bin/conda install -y python=3.11 pip && \
    /opt/conda/bin/conda clean -afy

ENV PATH="/opt/conda/bin:${PATH}"

RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    numpy==1.26.4 \
    pandas==2.3.3 \
    pyarrow==15.0.2 \
    PyMySQL==1.2.4

ENV PYSPARK_PYTHON=/opt/conda/bin/python
ENV PYSPARK_DRIVER_PYTHON=/opt/conda/bin/python

COPY ivy2/ /root/.ivy2/
WORKDIR /app
COPY features.py stream_transactions.py train_fraud_model.py /app/

CMD ["bash"]
