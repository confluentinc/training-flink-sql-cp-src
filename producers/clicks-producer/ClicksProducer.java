import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class ClicksProducer {

    private static final String TOPIC = "clicks";

    private static final String SCHEMA_JSON = "{"
            + "\"type\":\"record\","
            + "\"name\":\"clicks\","
            + "\"namespace\":\"io.confluent.training.avro\","
            + "\"fields\":["
            + "  {\"name\":\"ip\",\"type\":\"string\"},"
            + "  {\"name\":\"userid\",\"type\":\"int\"},"
            + "  {\"name\":\"remote_user\",\"type\":\"string\"},"
            + "  {\"name\":\"time\",\"type\":\"string\"},"
            + "  {\"name\":\"_time\",\"type\":\"long\"},"
            + "  {\"name\":\"request\",\"type\":\"string\"},"
            + "  {\"name\":\"status\",\"type\":\"string\"},"
            + "  {\"name\":\"bytes\",\"type\":\"string\"},"
            + "  {\"name\":\"referrer\",\"type\":\"string\"},"
            + "  {\"name\":\"agent\",\"type\":\"string\"}"
            + "]}";

    private static final String[] IPS = {
            "111.152.45.45", "111.203.236.146", "111.168.57.122", "111.249.79.93", "111.168.57.122",
            "111.90.225.227", "111.173.165.103", "111.145.8.144", "111.245.174.248", "111.245.174.111",
            "222.152.45.45", "222.203.236.146", "222.168.57.122", "222.249.79.93", "222.168.57.122",
            "222.90.225.227", "222.173.165.103", "222.145.8.144", "222.245.174.248", "222.245.174.222",
            "122.152.45.245", "122.203.236.246", "122.168.57.222", "122.249.79.233", "122.168.57.222",
            "122.90.225.227", "122.173.165.203", "122.145.8.244", "122.245.174.248", "122.245.174.122",
            "233.152.245.45", "233.203.236.146", "233.168.257.122", "233.249.279.93", "233.168.257.122",
            "233.90.225.227", "233.173.215.103", "233.145.28.144", "233.245.174.248", "233.245.174.233"
    };

    private static final int[] USERIDS = {102, 215, 308, 421, 537, 644, 718, 803, 956, 1047};

    private static final String[] REQUESTS = {
            "GET /index.html HTTP/1.1",
            "GET /site/user_status.html HTTP/1.1",
            "GET /site/login.html HTTP/1.1",
            "GET /site/user_status.html HTTP/1.1",
            "GET /images/track.png HTTP/1.1",
            "GET /images/logo-small.png HTTP/1.1"
    };

    private static final String[] STATUS_CODES = {"200", "302", "404", "405", "406", "407"};
    private static final String[] BYTES_VALUES = {"278", "1289", "2048", "4096", "4006", "4196", "14096"};
    private static final String[] USER_AGENTS = {
            "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36"
    };

    private static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.US);

    public static void main(String[] args) throws InterruptedException {
        String bootstrap = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9094");
        String schemaRegistryUrl = System.getenv().getOrDefault("SCHEMA_REGISTRY_URL", "http://localhost:8081");
        long delayMs = Long.parseLong(System.getenv().getOrDefault("DELAY_MS", "250"));

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "100");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        props.put("schema.registry.url", schemaRegistryUrl);

        Schema schema = new Schema.Parser().parse(SCHEMA_JSON);
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        long count = 0;

        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props)) {
            while (true) {
                String ip = IPS[rng.nextInt(IPS.length)];
                int userid = USERIDS[Math.abs(ip.hashCode()) % USERIDS.length];
                long epochMs = System.currentTimeMillis();
                String timeStr = TIME_FORMAT.format(new Date(epochMs));
                String request = REQUESTS[rng.nextInt(REQUESTS.length)];
                String status = STATUS_CODES[rng.nextInt(STATUS_CODES.length)];
                String bytes = BYTES_VALUES[rng.nextInt(BYTES_VALUES.length)];
                String agent = USER_AGENTS[rng.nextInt(USER_AGENTS.length)];

                GenericRecord record = new GenericData.Record(schema);
                record.put("ip", ip);
                record.put("userid", userid);
                record.put("remote_user", "-");
                record.put("time", timeStr);
                record.put("_time", epochMs);
                record.put("request", request);
                record.put("status", status);
                record.put("bytes", bytes);
                record.put("referrer", "-");
                record.put("agent", agent);

                String key = String.valueOf(userid);
                producer.send(new ProducerRecord<>(TOPIC, key, record), (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("ERROR: " + exception.getMessage());
                    }
                });

                count++;
                if (count % 100 == 0) {
                    System.out.printf("[%d] Producing clicks... (latest: UserID=%d, IP=%s)%n", count, userid, ip);
                }

                Thread.sleep(delayMs);
            }
        }
    }
}
