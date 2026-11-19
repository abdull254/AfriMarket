# Hadoop 2.8.2 (the same version the team's HDFS volume was created with) on a
# Java 8 base that has both x86_64 and ARM64 builds, so it runs natively on
# Apple Silicon / ARM Linux as well as on Intel/AMD machines.
FROM eclipse-temurin:8-jre-jammy

ARG HADOOP_VERSION=2.8.2

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl procps && \
    rm -rf /var/lib/apt/lists/*

RUN curl -fL https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
        -o /tmp/hadoop.tar.gz && \
    tar -xzf /tmp/hadoop.tar.gz -C / && \
    rm /tmp/hadoop.tar.gz && \
    ln -s /hadoop-${HADOOP_VERSION} /hadoop && \
    rm -rf /hadoop/share/doc

ENV HADOOP_HOME=/hadoop \
    HADOOP_CONF_DIR=/hadoop/etc/hadoop \
    HADOOP_LOG_DIR=/hadoop/logs \
    YARN_LOG_DIR=/hadoop/logs \
    PATH="/hadoop/bin:/hadoop/sbin:${PATH}"

COPY conf/core-site.xml /hadoop/etc/hadoop/core-site.xml
COPY conf/hdfs-site.xml /hadoop/etc/hadoop/hdfs-site.xml
COPY start-hadoop.sh /start-hadoop.sh
RUN chmod +x /start-hadoop.sh

# 50070 NameNode UI, 8020 NameNode RPC, 50010/50075 DataNode, 8088 YARN UI
EXPOSE 50070 8020 50010 50075 8088

ENTRYPOINT ["/start-hadoop.sh"]
