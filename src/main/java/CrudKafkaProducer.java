import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class CrudKafkaProducer {
    private KafkaProducer<String, String> producer;
    private ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(CrudKafkaProducer.class);

    public CrudKafkaProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        this.producer = new KafkaProducer<>(props);
    }

    public void sendCreate(String topic, Object entity) throws Exception {
        String message = objectMapper.writeValueAsString(entity);
        producer.send(new ProducerRecord<>(topic, "create", message));
    }

    public void sendRead(String topic, String id) {
        producer.send(new ProducerRecord<>(topic, "read", id));
    }

    public void sendUpdate(String topic, Object entity) throws Exception {
        String message = objectMapper.writeValueAsString(entity);
        producer.send(new ProducerRecord<>(topic, "update", message));
    }

    public void sendDelete(String topic, String id) {
        producer.send(new ProducerRecord<>(topic, "delete", id));
    }

    public void close() {
        producer.close();
        logger.info("Producer closed");
    }
}
