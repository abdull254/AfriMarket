#!/bin/bash
# Starts the NameNode, DataNode and YARN ResourceManager directly, without SSH.
# (The old image started the DataNode over SSH to localhost, which could fail
# silently and leave HDFS with no DataNode.)
set -euo pipefail

mkdir -p "$HADOOP_LOG_DIR"
# PID files left by a previous run of this container would make the start
# scripts think the daemons are already running.
rm -f /tmp/hadoop-*.pid /tmp/yarn-*.pid

# A brand-new, empty HDFS volume is formatted once. An existing volume is never touched.
if [ ! -d /tmp/hadoop-root/dfs/name/current ]; then
    echo "No NameNode metadata found: formatting a new HDFS volume"
    hdfs namenode -format -nonInteractive
fi

hadoop-daemon.sh start namenode
hadoop-daemon.sh start datanode
yarn-daemon.sh start resourcemanager

# Keep the container running. If the NameNode or DataNode stops, show why and
# exit, so Docker restarts the container (restart: unless-stopped).
while sleep 30; do
    for daemon in namenode datanode; do
        if ! pgrep -f "proc_${daemon}" >/dev/null; then
            echo "HDFS ${daemon} stopped. Last log lines:"
            tail -n 50 "$HADOOP_LOG_DIR"/*"${daemon}"*.log || true
            exit 1
        fi
    done
done
