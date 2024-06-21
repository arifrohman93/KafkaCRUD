public class CrudKafka {
        public static void main(String[] args) throws Exception {
            String bootstrapServers = "localhost:9092";
            String topic = "crud_operations";

            CrudKafkaConsumer consumer = new CrudKafkaConsumer(bootstrapServers, "crud_group", topic);
            new Thread(() -> consumer.listen()).start();

            CrudKafkaProducer producer = new CrudKafkaProducer(bootstrapServers);
            producer.sendCreate(topic, new Entity(1, "Create Entity"));
            producer.sendRead(topic, "1");
            producer.sendUpdate(topic, new Entity(1, "Updated Entity"));
            producer.sendDelete(topic, "1");

            producer.close();
            consumer.close();
        }
    }

    class Entity {
        private int id;
        private String name;

        public Entity(int id, String name) {
            this.id = id;
            this.name = name;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

}
