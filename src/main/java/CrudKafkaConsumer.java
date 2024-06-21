import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class CrudKafkaConsumer {
    private KafkaConsumer<String, String> consumer;
    private ObjectMapper objectMapper = new ObjectMapper();

    public CrudKafkaConsumer(String bootstrapServers, String groupId, String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        this.consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
    }

    public void listen() {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                handleRecord(record);
            }
        }
    }

    private void handleRecord(ConsumerRecord<String, String> record) {
        String key = record.key();
        String value = record.value();

        switch (key) {
            case "create":
                System.out.println("Create: " + value);
                break;
            case "read":
                System.out.println("Read: " + value);
                break;
            case "update":
                System.out.println("Update: " + value);
                break;
            case "delete":
                System.out.println("Delete: " + value);
                break;
            default:
                throw new IllegalArgumentException("Unexpected value: " + key);
        }
    }

    public void close() {
        consumer.close();
    }
}
