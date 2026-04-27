import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class ShoeProducer {

    private static final String CUSTOMERS_SCHEMA_JSON = "{"
            + "\"type\":\"record\","
            + "\"name\":\"shoe_customers\","
            + "\"namespace\":\"io.confluent.training.avro\","
            + "\"fields\":["
            + "  {\"name\":\"id\",\"type\":\"string\"},"
            + "  {\"name\":\"first_name\",\"type\":\"string\"},"
            + "  {\"name\":\"last_name\",\"type\":\"string\"},"
            + "  {\"name\":\"email\",\"type\":\"string\"},"
            + "  {\"name\":\"phone\",\"type\":\"string\"},"
            + "  {\"name\":\"street_address\",\"type\":\"string\"},"
            + "  {\"name\":\"state\",\"type\":\"string\"},"
            + "  {\"name\":\"zip_code\",\"type\":\"string\"},"
            + "  {\"name\":\"country\",\"type\":\"string\"},"
            + "  {\"name\":\"country_code\",\"type\":\"string\"}"
            + "]}";

    private static final String PRODUCTS_SCHEMA_JSON = "{"
            + "\"type\":\"record\","
            + "\"name\":\"shoe_product\","
            + "\"namespace\":\"io.confluent.training.avro\","
            + "\"fields\":["
            + "  {\"name\":\"id\",\"type\":\"string\"},"
            + "  {\"name\":\"brand\",\"type\":\"string\"},"
            + "  {\"name\":\"name\",\"type\":\"string\"},"
            + "  {\"name\":\"sale_price\",\"type\":\"int\"},"
            + "  {\"name\":\"rating\",\"type\":\"double\"}"
            + "]}";

    private static final String ORDERS_SCHEMA_JSON = "{"
            + "\"type\":\"record\","
            + "\"name\":\"shoe_orders\","
            + "\"namespace\":\"io.confluent.training.avro\","
            + "\"fields\":["
            + "  {\"name\":\"order_id\",\"type\":\"int\"},"
            + "  {\"name\":\"product_id\",\"type\":\"string\"},"
            + "  {\"name\":\"customer_id\",\"type\":\"string\"},"
            + "  {\"name\":\"ts\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}}"
            + "]}";

    private static final String CLICKSTREAM_SCHEMA_JSON = "{"
            + "\"type\":\"record\","
            + "\"name\":\"shoe_clickstream\","
            + "\"namespace\":\"io.confluent.training.avro\","
            + "\"fields\":["
            + "  {\"name\":\"product_id\",\"type\":\"string\"},"
            + "  {\"name\":\"user_id\",\"type\":\"string\"},"
            + "  {\"name\":\"view_time\",\"type\":\"int\"},"
            + "  {\"name\":\"page_url\",\"type\":\"string\"},"
            + "  {\"name\":\"ip\",\"type\":\"string\"},"
            + "  {\"name\":\"ts\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}}"
            + "]}";

    private static List<String[]> loadCsv(String path) throws IOException {
        List<String[]> rows = new ArrayList<>();
        for (String line : Files.readAllLines(Paths.get(path))) {
            line = line.trim();
            if (!line.isEmpty()) rows.add(line.split("\\|", -1));
        }
        return rows;
    }

    public static void main(String[] args) throws Exception {
        String bootstrap = env("BOOTSTRAP_SERVERS", "localhost:9094");
        String schemaRegistryUrl = env("SCHEMA_REGISTRY_URL", "http://localhost:8081");
        long delayMs = Long.parseLong(env("DELAY_MS", "1000"));
        int count = Integer.parseInt(env("COUNT", "0")); // 0 = infinite

        String scriptDir = System.getProperty("shoe.producer.dir", ".");
        List<String[]> customers = loadCsv(scriptDir + "/customers.csv");
        List<String[]> products = loadCsv(scriptDir + "/products.csv");

        // Build separate lists of customer IDs and product IDs for order generation
        Set<String> customerIdSet = new HashSet<>();
        for (String[] c : customers) customerIdSet.add(c[0]);
        String[] customerIds = customerIdSet.toArray(new String[0]);

        Set<String> productIdSet = new HashSet<>();
        for (String[] p : products) productIdSet.add(p[1]); // products.csv: sale_price|id|brand|name|rating
        String[] productIds = productIdSet.toArray(new String[0]);

        System.out.printf("Loaded %d customers, %d products (%d unique customer IDs, %d unique product IDs)%n",
                customers.size(), products.size(), customerIds.length, productIds.length);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "50");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        props.put("schema.registry.url", schemaRegistryUrl);

        Schema custSchema = new Schema.Parser().parse(CUSTOMERS_SCHEMA_JSON);
        Schema prodSchema = new Schema.Parser().parse(PRODUCTS_SCHEMA_JSON);
        Schema orderSchema = new Schema.Parser().parse(ORDERS_SCHEMA_JSON);
        Schema clickSchema = new Schema.Parser().parse(CLICKSTREAM_SCHEMA_JSON);

        ThreadLocalRandom rng = ThreadLocalRandom.current();
        int orderIdCounter = 1000;
        long syntheticTsStart = System.currentTimeMillis();
        long orderTsStep = 100000L;            // +100 seconds per order (matches CC Datagen shoe_orders)
        long clickTsStep = 10000L;             // +10 seconds per click (matches CC Datagen shoe_clickstream)
        int clicksPerOrder = 10;               // 10 clicks per order keeps event-time ranges aligned
        long iteration = 0;
        long clickCounter = 0;

        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props)) {
            while (count == 0 || iteration < count) {
                iteration++;

                // 1. Customer record
                String[] c = customers.get(rng.nextInt(customers.size()));
                GenericRecord custRec = new GenericData.Record(custSchema);
                custRec.put("id", c[0]);
                custRec.put("first_name", c[1]);
                custRec.put("last_name", c[2]);
                custRec.put("email", c[3]);
                custRec.put("phone", c[4]);
                custRec.put("street_address", c[5]);
                custRec.put("state", c[6]);
                custRec.put("zip_code", c[7]);
                custRec.put("country", c[8]);
                custRec.put("country_code", c[9]);
                producer.send(new ProducerRecord<>("shoe-customers", c[0], custRec));

                // 2. Product record (CSV: sale_price|id|brand|name|rating)
                String[] p = products.get(rng.nextInt(products.size()));
                GenericRecord prodRec = new GenericData.Record(prodSchema);
                prodRec.put("id", p[1]);
                prodRec.put("brand", p[2]);
                prodRec.put("name", p[3]);
                prodRec.put("sale_price", Integer.parseInt(p[0]));
                prodRec.put("rating", Double.parseDouble(p[4]));
                producer.send(new ProducerRecord<>("shoe-products", p[1], prodRec));

                // 3. Order record
                int orderId = orderIdCounter++;
                String orderProductId = productIds[rng.nextInt(productIds.length)];
                String orderCustomerId = customerIds[rng.nextInt(customerIds.length)];
                long orderTs = syntheticTsStart + (iteration * orderTsStep);

                GenericRecord orderRec = new GenericData.Record(orderSchema);
                orderRec.put("order_id", orderId);
                orderRec.put("product_id", orderProductId);
                orderRec.put("customer_id", orderCustomerId);
                orderRec.put("ts", orderTs);
                producer.send(new ProducerRecord<>("shoe-orders", String.valueOf(orderId), orderRec));

                // 4. Clickstream records (10 per order, matching CC Datagen rate ratio)
                for (int ci = 0; ci < clicksPerOrder; ci++) {
                    clickCounter++;
                    long clickTs = syntheticTsStart + (clickCounter * clickTsStep);
                    String clickProductId = productIds[rng.nextInt(productIds.length)];
                    String clickUserId = customerIds[rng.nextInt(customerIds.length)];
                    GenericRecord clickRec = new GenericData.Record(clickSchema);
                    clickRec.put("product_id", clickProductId);
                    clickRec.put("user_id", clickUserId);
                    clickRec.put("view_time", rng.nextInt(10, 121));
                    clickRec.put("page_url", "https://www.acme.com/product/" + randomLowerAlpha(rng, 5));
                    clickRec.put("ip", rng.nextInt(1, 255) + "." + rng.nextInt(0, 256) + "." + rng.nextInt(0, 256) + "." + rng.nextInt(1, 255));
                    clickRec.put("ts", clickTs);
                    producer.send(new ProducerRecord<>("shoe-clickstream", clickUserId, clickRec));
                }

                if (iteration % 100 == 0) {
                    System.out.printf("[%d] Producing shoe data... (customer=%s, product=%s, order=%d)%n",
                            iteration, c[0].substring(0, 8), p[1].substring(0, 8), orderId);
                }

                if (delayMs > 0) Thread.sleep(delayMs);
            }
        }

        System.out.printf("Completed: %d iterations produced.%n", iteration);
    }

    private static String env(String key, String defaultVal) {
        String v = System.getenv(key);
        return (v != null && !v.isEmpty()) ? v : defaultVal;
    }

    private static String randomLowerAlpha(ThreadLocalRandom rng, int len) {
        char[] chars = new char[len];
        for (int i = 0; i < len; i++) chars[i] = (char) ('a' + rng.nextInt(26));
        return new String(chars);
    }
}
