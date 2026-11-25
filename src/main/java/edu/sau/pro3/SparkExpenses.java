package edu.sau.pro3;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.time.ZoneId;
import java.util.Map;
import java.util.concurrent.Executors;

import static org.apache.spark.sql.functions.*;

public final class SparkExpenses {
    private static final String CASSANDRA = "org.apache.spark.sql.cassandra";
    private static final Map<String, String> TABLE = Map.of("keyspace", "sau_pro3", "table", "expenses");
    private static final ObjectMapper JSON = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException("Expected Kafka bootstrap servers, checkpoint path and report port");
        }
        try (SparkSession spark = SparkSession.builder()
                .appName("SAU Project 3 Expenses")
                .config("spark.sql.session.timeZone", "Europe/Istanbul")
                .config("spark.sql.shuffle.partitions", "2")
                .getOrCreate()) {
            spark.sparkContext().setLogLevel("WARN");
            var kafka = spark.readStream().format("kafka")
                    .option("kafka.bootstrap.servers", args[0])
                    .option("subscribe", "accounting,research,sales,operations")
                    .option("startingOffsets", "earliest")
                    .option("maxOffsetsPerTrigger", "1000")
                    .load();
            StreamingQuery ingest = parse(kafka).writeStream()
                    .option("checkpointLocation", args[1])
                    .trigger(Trigger.ProcessingTime("1 second"))
                    .foreachBatch((org.apache.spark.api.java.function.VoidFunction2<Dataset<Row>, Long>) (batch, id) ->
                            batch.write().format(CASSANDRA).options(TABLE).mode("append").save())
                    .start();
            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", Integer.parseInt(args[2])), 0);
            var executor = Executors.newSingleThreadExecutor();
            server.setExecutor(executor);
            server.createContext("/health", exchange -> send(exchange, ingest.isActive() ? 200 : 503,
                    Map.of("streaming", ingest.isActive())));
            server.createContext("/reports/", exchange -> handleReport(exchange, spark));
            server.start();
            try {
                ingest.awaitTermination();
            } finally {
                server.stop(0);
                executor.shutdownNow();
                if (ingest.isActive()) {
                    ingest.stop();
                }
            }
        }
    }

    static Dataset<Row> parse(Dataset<Row> kafka) {
        return kafka.selectExpr("topic", "partition AS kafka_partition", "offset AS kafka_offset",
                        "CAST(value AS STRING) AS csv")
                .withColumn("expense", from_csv(col("csv"),
                        lit("empno STRING, occurred_at STRING, description STRING, expense_type STRING, "
                                + "quantity STRING, payment STRING, extra STRING"),
                        Map.of("ignoreLeadingWhiteSpace", "true", "ignoreTrailingWhiteSpace", "true",
                                "escape", "\"")))
                .selectExpr("topic", "kafka_partition", "kafka_offset", "expense.*")
                .where("extra IS NULL AND trim(payment) RLIKE '^[0-9]{1,10}([.][0-9]{1,2})?$'")
                .selectExpr("topic", "kafka_partition", "kafka_offset",
                        "TRY_CAST(trim(empno) AS INT) AS empno",
                        "TRY_CAST(trim(occurred_at) AS TIMESTAMP) AS occurred_at",
                        "trim(description) AS description", "trim(expense_type) AS expense_type",
                        "TRY_CAST(trim(quantity) AS INT) AS quantity",
                        "TRY_CAST(trim(payment) AS DECIMAL(12,2)) AS payment")
                .where("empno > 0 AND occurred_at IS NOT NULL AND length(description) > 0 "
                        + "AND length(expense_type) > 0 AND quantity > 0 AND payment IS NOT NULL");
    }

    private static void handleReport(HttpExchange exchange, SparkSession spark) throws IOException {
        String[] path = exchange.getRequestURI().getPath().split("/", -1);
        if (!exchange.getRequestMethod().equals("GET") || path.length != 4) {
            send(exchange, 404, Map.of("error", "Expected /reports/{empno}/{page}"));
            return;
        }
        int empno;
        int page;
        try {
            empno = Integer.parseInt(path[2]);
            page = Integer.parseInt(path[3]);
            if (empno < 1 || page < 1 || page > Integer.MAX_VALUE / ExpenseReport.PAGE_SIZE) {
                throw new NumberFormatException("Value out of bounds");
            }
        } catch (NumberFormatException error) {
            send(exchange, 400, Map.of("error", "Invalid employee or page"));
            return;
        }
        ExpenseReport report;
        try {
            report = report(spark, empno, page);
        } catch (Exception error) {
            System.err.println("Spark could not calculate the report for employee " + empno);
            error.printStackTrace(System.err);
            send(exchange, 503, Map.of("error", "Spark report unavailable"));
            return;
        }
        send(exchange, 200, report);
    }

    private static ExpenseReport report(SparkSession spark, int empno, int page) {
        Dataset<Row> matching = spark.read().format(CASSANDRA).options(TABLE).load()
                .where(col("empno").equalTo(empno)).cache();
        try {
            Row summary = matching.agg(sum("payment"), count(lit(1))).first();
            BigDecimal total = summary.isNullAt(0) ? new BigDecimal("0.00") : summary.getDecimal(0);
            var expenses = matching.orderBy(desc("occurred_at"), asc("topic"), asc("kafka_partition"), desc("kafka_offset"))
                    .offset((page - 1) * ExpenseReport.PAGE_SIZE).limit(ExpenseReport.PAGE_SIZE)
                    .select("occurred_at", "description", "expense_type", "quantity", "payment")
                    .collectAsList().stream()
                    .map(row -> new ExpenseReport.Expense(row.getTimestamp(0).toInstant()
                            .atZone(ZoneId.of("Europe/Istanbul")).toOffsetDateTime().toString(),
                            row.getString(1), row.getString(2), row.getInt(3), row.getDecimal(4)))
                    .toList();
            return new ExpenseReport(total, summary.getLong(1), expenses);
        } finally {
            matching.unpersist();
        }
    }

    private static void send(HttpExchange exchange, int status, Object body) throws IOException {
        byte[] bytes = JSON.writeValueAsBytes(body);
        try (exchange) {
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, bytes.length);
            exchange.getResponseBody().write(bytes);
        }
    }
}
