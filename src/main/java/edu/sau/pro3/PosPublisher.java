package edu.sau.pro3;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public final class PosPublisher {
    private static final String[][] PRODUCTS = {
            {"Pasta", "Food"}, {"Refrigerator", "Appliances"},
            {"Cinema", "Entertainment"}, {"Book", "Stationery"}
    };

    public static void main(String[] args) throws Exception {
        boolean demo = (args.length == 1 || args.length == 2) && args[0].equals("--demo");
        boolean stdin = args.length == 1 && args[0].equals("--stdin");
        if (!demo && !stdin) {
            throw new IllegalArgumentException("Usage: scripts/pos.sh --demo [empno] | --stdin");
        }

        Integer selectedEmployee = null;
        if (demo && args.length == 2) {
            try {
                selectedEmployee = Integer.valueOf(args[1]);
            } catch (NumberFormatException error) {
                throw new IllegalArgumentException("Employee number must be an integer.", error);
            }
        }

        Map<Integer, String> departments = departments();
        if (selectedEmployee != null && !departments.containsKey(selectedEmployee)) {
            throw new IllegalArgumentException("Unknown employee: " + selectedEmployee);
        }
        Map<String, Object> config = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, env("PRO3_KAFKA", "127.0.0.1:9092"),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 15000,
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000,
                ProducerConfig.MAX_BLOCK_MS_CONFIG, 15000);
        try (var producer = new KafkaProducer<String, String>(config)) {
            if (demo) {
                generate(producer, departments, selectedEmployee);
            } else {
                var input = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
                for (String line; (line = input.readLine()) != null; ) {
                    if (!line.isBlank()) {
                        int empno = Integer.parseInt(line.split(",", 2)[0].strip());
                        publish(producer, departments, empno, line);
                    }
                }
            }
        }
    }

    private static void generate(KafkaProducer<String, String> producer, Map<Integer, String> departments,
                                 Integer selectedEmployee)
            throws Exception {
        var personnel = departments.keySet().stream().toList();
        var random = ThreadLocalRandom.current();
        while (!Thread.currentThread().isInterrupted()) {
            int empno = selectedEmployee != null ? selectedEmployee : personnel.get(random.nextInt(personnel.size()));
            String[] product = PRODUCTS[random.nextInt(PRODUCTS.length)];
            int quantity = random.nextInt(1, 5);
            BigDecimal payment = BigDecimal.valueOf(quantity * random.nextLong(500, 25001), 2);
            String timestamp = OffsetDateTime.now(ZoneId.of("Europe/Istanbul")).truncatedTo(ChronoUnit.SECONDS).toString();
            String line = String.join(",", Integer.toString(empno), timestamp, product[0], product[1],
                    Integer.toString(quantity), payment.toPlainString());
            publish(producer, departments, empno, line);
            Thread.sleep(1000);
        }
    }

    private static void publish(KafkaProducer<String, String> producer, Map<Integer, String> departments,
                                int empno, String value) throws Exception {
        String topic = departments.get(empno);
        if (topic == null) {
            throw new IllegalArgumentException("Unknown employee: " + empno);
        }
        producer.send(new ProducerRecord<>(topic, Integer.toString(empno), value)).get();
        System.out.println(topic + " " + value);
    }

    private static Map<Integer, String> departments() throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        var departments = new HashMap<Integer, String>();
        try (var connection = DriverManager.getConnection(
                env("PRO3_DB_URL", "jdbc:mysql://127.0.0.1:3307/sau_pro3?connectionTimeZone=UTC"),
                env("PRO3_DB_USER", "pro3"), env("PRO3_DB_PASSWORD", "pro3-local"));
             var statement = connection.createStatement();
             var rows = statement.executeQuery("SELECT e.empno, d.dname FROM emp e JOIN dept d USING (deptno)")) {
            while (rows.next()) {
                departments.put(rows.getInt(1), rows.getString(2).toLowerCase(Locale.ROOT));
            }
        }
        if (departments.isEmpty()) {
            throw new IllegalStateException("MySQL did not return any employees");
        }
        return departments;
    }

    private static String env(String name, String defaultValue) {
        return System.getenv().getOrDefault(name, defaultValue);
    }
}
