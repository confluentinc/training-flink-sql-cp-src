import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class OrdersProducer {

    private static final String TOPIC = "orders";

    private static final String SCHEMA_JSON = "{"
            + "\"type\":\"record\","
            + "\"name\":\"orders\","
            + "\"namespace\":\"io.confluent.training.avro\","
            + "\"fields\":["
            + "  {\"name\":\"ordertime\",\"type\":\"long\"},"
            + "  {\"name\":\"orderid\",\"type\":\"int\"},"
            + "  {\"name\":\"itemid\",\"type\":\"string\"},"
            + "  {\"name\":\"orderunits\",\"type\":\"double\"},"
            + "  {\"name\":\"address\",\"type\":{\"type\":\"record\",\"name\":\"address\","
            + "    \"fields\":["
            + "      {\"name\":\"city\",\"type\":\"string\"},"
            + "      {\"name\":\"state\",\"type\":\"string\"},"
            + "      {\"name\":\"zipcode\",\"type\":\"long\"}"
            + "    ]"
            + "  }}"
            + "]}";

    private static final String[] CITIES  = {"Seattle","Denver","Austin","Miami","Chicago","Portland","New York","Los Angeles","Boston","Phoenix"};
    private static final String[] STATES  = {"WA","CO","TX","FL","IL","OR","NY","CA","MA","AZ"};
    private static final int[]    ZIP_PFX = {981, 802, 787, 331, 606, 972, 100, 900, 21, 850};
    private static final int[]    ZIP_LO  = {0, 1, 1, 1, 1, 1, 1, 1, 8, 1};
    private static final int[]    ZIP_HI  = {99, 39, 59, 99, 61, 29, 99, 89, 99, 99};

    public static void main(String[] args) throws InterruptedException {
        String bootstrap = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9094");
        String schemaRegistryUrl = System.getenv().getOrDefault("SCHEMA_REGISTRY_URL", "http://localhost:8081");
        long delayMs = Long.parseLong(System.getenv().getOrDefault("DELAY_MS", "500"));

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "100");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        props.put("schema.registry.url", schemaRegistryUrl);

        Schema schema = new Schema.Parser().parse(SCHEMA_JSON);
        Schema addressSchema = schema.getField("address").schema();
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        int orderId = 1;

        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props)) {
            System.out.printf("Producing to '%s' (delay=%dms, bootstrap=%s)%n", TOPIC, delayMs, bootstrap);

            while (true) {
                int idx = rng.nextInt(CITIES.length);
                long zipcode = (long) ZIP_PFX[idx] * 100 + ZIP_LO[idx] + rng.nextInt(ZIP_HI[idx] - ZIP_LO[idx] + 1);

                GenericRecord address = new GenericData.Record(addressSchema);
                address.put("city", CITIES[idx]);
                address.put("state", STATES[idx]);
                address.put("zipcode", zipcode);

                long ordertime = System.currentTimeMillis();
                String itemid = "Item_" + (1 + rng.nextInt(999));
                double orderunits = Math.round(rng.nextDouble(0.1, 25.0) * 100.0) / 100.0;

                GenericRecord record = new GenericData.Record(schema);
                record.put("ordertime", ordertime);
                record.put("orderid", orderId);
                record.put("itemid", itemid);
                record.put("orderunits", orderunits);
                record.put("address", address);

                String key = String.valueOf(orderId);

                // Add Kafka headers
                List<org.apache.kafka.common.header.Header> headers = Arrays.asList(
                    new RecordHeader("orders.producer.id", "1".getBytes(StandardCharsets.UTF_8)),
                    new RecordHeader("current.iteration", String.valueOf(orderId).getBytes(StandardCharsets.UTF_8))
                );

                ProducerRecord<String, GenericRecord> producerRecord =
                    new ProducerRecord<>(TOPIC, null, key, record, headers);

                producer.send(producerRecord, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("ERROR: " + exception.getMessage());
                    }
                });

                System.out.printf("Sent: key='%s' value={\"ordertime\":%d,\"orderid\":%d,\"itemid\":\"%s\",\"orderunits\":%.2f,\"address\":{\"city\":\"%s\",\"state\":\"%s\",\"zipcode\":%d}}%n",
                        key, ordertime, orderId, itemid, orderunits, CITIES[idx], STATES[idx], zipcode);

                orderId++;
                Thread.sleep(delayMs);
            }
        }
    }
}
