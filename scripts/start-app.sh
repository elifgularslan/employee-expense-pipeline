#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/env.sh"
mkdir -p "$run" "$logs"
cd "$project"
bash ./gradlew --no-daemon --max-workers=2 build
install -m 0644 "$artifact.jar" "$build.jar.new"
mv -f -- "$build.jar.new" "$build.jar"
install -m 0644 "$artifact-boot.jar" "$build-boot.jar.new"
mv -f -- "$build-boot.jar.new" "$build-boot.jar"
install -m 0644 "$project/scripts/ivy.xml" "$run/ivy.xml"
ivy_uri=$(python3 -c 'import sys; from pathlib import Path; print(Path(sys.argv[1]).as_uri())' "$run/ivy.xml")
launch spark-job "$spark/bin/spark-submit" \
    --master spark://127.0.0.1:7077 --deploy-mode client \
    --driver-memory 1g --executor-memory 768m --executor-cores 2 --total-executor-cores 2 \
    --conf spark.driver.host=127.0.0.1 --conf spark.driver.bindAddress=127.0.0.1 \
    --conf spark.cassandra.connection.host=127.0.0.1 \
    --conf "spark.jars.ivySettings=$ivy_uri" \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
    --class edu.sau.pro3.SparkExpenses "$build.jar" \
    127.0.0.1:9092 "file://$pro3_home/state/stream-checkpoint" 8093
wait_for "Spark Streaming" 150 curl -fsS --max-time 3 http://127.0.0.1:8093/health
if [[ ${1:-} != --spark-only ]]; then
    launch web java -Djava.net.preferIPv4Stack=true -jar "$build-boot.jar"
    wait_for "Spring Boot" 60 curl -fsS --max-time 5 http://127.0.0.1:8083/
    echo "Web application: http://localhost:8083/"
fi
