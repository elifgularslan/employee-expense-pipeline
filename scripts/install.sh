#!/usr/bin/env bash
set -euo pipefail
home=${PRO3_HOME:-"$HOME/.local/share/sau-pro3"}
mkdir -p "$home/dist"

download() {
    local directory=$1 artifact=$2 relative=$3
    shift 3
    [[ -d "$home/dist/$directory" ]] && return 0
    local base archive="$home/dist/$artifact.part"
    for base in https://dlcdn.apache.org https://archive.apache.org/dist "$@"; do
        if curl -fsSL --connect-timeout 8 --max-time 600 -o "$archive" "$base/$relative/$artifact"; then
            tar -xf "$archive" -C "$home/dist"
            rm -- "$archive"
            echo "$directory kuruldu."
            return 0
        fi
    done
    return 1
}

download spark-3.5.8-bin-hadoop3 spark-3.5.8-bin-hadoop3.tgz spark/spark-3.5.8 https://repo.huaweicloud.com/apache &
spark=$!
download kafka_2.13-3.9.1 kafka_2.13-3.9.1.tgz kafka/3.9.1 https://repo.huaweicloud.com/apache &
kafka=$!
download apache-cassandra-5.0.9 apache-cassandra-5.0.9-bin.tar.gz cassandra/5.0.9 https://repo.huaweicloud.com/apache &
cassandra=$!

failed=0
for pid in "$spark" "$kafka" "$cassandra"; do
    wait "$pid" || failed=1
done
exit "$failed"
