#!/usr/bin/env bash
project=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
pro3_home=${PRO3_HOME:-"$HOME/.local/share/sau-pro3"}
export JAVA_HOME=${PRO3_JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}
export PATH="$JAVA_HOME/bin:$PATH"
export SPARK_LOCAL_IP=127.0.0.1
hadoop=${HADOOP_HOME:-"$HOME/hadoop"}
kafka="$pro3_home/dist/kafka_2.13-3.9.1"
cassandra="$pro3_home/dist/apache-cassandra-5.0.9"
spark="$pro3_home/dist/spark-3.5.8-bin-hadoop3"
run="$pro3_home/run"
logs="$pro3_home/logs"
artifact="$project/build/libs/bigdata-pro3-1.0.0"
build="$run/bigdata-pro3-1.0.0"

wait_for() {
    local label=$1 timeout=$2
    shift 2
    local deadline=$((SECONDS + timeout))
    until "$@" >/dev/null 2>&1; do
        if (( SECONDS >= deadline )); then
            echo "$label not ready. Logs directory: $logs" >&2
            return 1
        fi
        sleep 2
    done
    echo "$label ready."
}

launch() {
    local name=$1
    shift
    if [[ -r "$run/$name.pid" ]]; then
        local state
        state=$(ps -p "$(cat "$run/$name.pid")" -o stat= || true)
        if [[ -n $state && $state != Z* ]]; then
            return
        fi
    fi
    nohup "$@" >>"$logs/$name.log" 2>&1 </dev/null &
    echo "$!" >"$run/$name.pid"
}
