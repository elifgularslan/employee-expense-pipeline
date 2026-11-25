#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/env.sh"
mkdir -p "$run" "$logs" "$pro3_home/state/mysql"

if [[ ! -f "$pro3_home/state/mysql/mysql.ibd" ]]; then
    mysqld --no-defaults --initialize-insecure --datadir="$pro3_home/state/mysql" >"$logs/mysql-init.log" 2>&1
fi
launch mysql mysqld --no-defaults --datadir="$pro3_home/state/mysql" --port=3307 \
    --socket="$run/mysql.sock" --pid-file="$run/mysqld.pid" --bind-address=127.0.0.1 \
    --mysqlx=OFF --innodb-buffer-pool-size=64M --skip-log-bin
mysql_root=(mysql --no-defaults --socket="$run/mysql.sock" -uroot)
wait_for MySQL 45 "${mysql_root[@]}" -e "SELECT 1"
python3 "$project/scripts/seed-mysql.py" | "${mysql_root[@]}"

export HADOOP_HEAPSIZE_MAX=${HADOOP_HEAPSIZE_MAX:-512}
if ! timeout 8 "$hadoop/bin/hdfs" dfsadmin -safemode get >/dev/null 2>&1; then
    launch hdfs-namenode "$hadoop/bin/hdfs" namenode
fi
live_datanode() {
    timeout 8 "$hadoop/bin/hdfs" dfsadmin -report -live | grep -q '^Live datanodes ([1-9]'
}
if ! live_datanode 2>/dev/null; then
    launch hdfs-datanode "$hadoop/bin/hdfs" datanode
fi
wait_for HDFS 90 "$hadoop/bin/hdfs" dfs -ls /
wait_for DataNode 60 live_datanode
timeout 90 "$hadoop/bin/hdfs" dfsadmin -safemode wait
"$hadoop/bin/hdfs" dfs -mkdir -p /sau-pro3/photos
if ! "$hadoop/bin/hdfs" dfs -test -e /sau-pro3/photos/placeholder.png; then
    "$hadoop/bin/hdfs" dfs -put - /sau-pro3/photos/placeholder.png <"$project/data/placeholder.png"
fi

cat >"$run/kafka.properties" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:9093
listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093
advertised.listeners=PLAINTEXT://127.0.0.1:9092
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
controller.listener.names=CONTROLLER
log.dirs=$pro3_home/state/kafka
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
auto.create.topics.enable=false
EOF
if [[ ! -f "$pro3_home/state/kafka/meta.properties" ]]; then
    "$kafka/bin/kafka-storage.sh" format -t "$("$kafka/bin/kafka-storage.sh" random-uuid)" -c "$run/kafka.properties"
fi
launch kafka env KAFKA_HEAP_OPTS="-Xms256m -Xmx512m" LOG_DIR="$logs" \
    "$kafka/bin/kafka-server-start.sh" "$run/kafka.properties"

if ! "$cassandra/bin/cqlsh" 127.0.0.1 9042 -e "DESCRIBE KEYSPACES" >/dev/null 2>&1; then
    launch cassandra env MAX_HEAP_SIZE=768M HEAP_NEWSIZE=192M CASSANDRA_LOG_DIR="$logs" \
        "$cassandra/bin/cassandra" -f "-Dcassandra.storagedir=$pro3_home/state/cassandra"
fi

export SPARK_PID_DIR="$run" SPARK_LOG_DIR="$logs" SPARK_IDENT_STRING=sau-pro3
export SPARK_DAEMON_MEMORY=384m
if ! curl -fsS --max-time 3 http://127.0.0.1:8080/json/ >/dev/null; then
    "$spark/sbin/start-master.sh" --host 127.0.0.1 --port 7077 --webui-port 8080
fi
if ! curl -fsS --max-time 3 http://127.0.0.1:8081/json/ >/dev/null; then
    "$spark/sbin/start-worker.sh" --host 127.0.0.1 --cores 2 --memory 1536m \
        --work-dir "$pro3_home/state/spark-worker" --webui-port 8081 spark://127.0.0.1:7077
fi

wait_for Kafka 90 "$kafka/bin/kafka-topics.sh" --bootstrap-server 127.0.0.1:9092 --list
for topic in accounting research sales operations; do
    "$kafka/bin/kafka-topics.sh" --bootstrap-server 127.0.0.1:9092 --create --if-not-exists \
        --topic "$topic" --partitions 1 --replication-factor 1
done
wait_for Cassandra 120 "$cassandra/bin/cqlsh" 127.0.0.1 9042 -e "DESCRIBE KEYSPACES"
"$cassandra/bin/cqlsh" 127.0.0.1 9042 -f "$project/data/schema.cql"
spark_worker_ready() {
    curl -fsS --max-time 3 http://127.0.0.1:8080/json/ |
        python3 -c 'import json, sys; sys.exit(json.load(sys.stdin)["aliveworkers"] < 1)'
}
wait_for Spark 60 spark_worker_ready
