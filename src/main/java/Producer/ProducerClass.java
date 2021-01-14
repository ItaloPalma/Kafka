package Producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerClass {

    public static void main(String[] args) {

        //* https://kafka.apache.org/documentation/#producerconfigs

        String bootstrapServer = "127.0.0.1:9092";

        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);

        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "Mensagem Teste!");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        producer.send(record);
        producer.flush();
        producer.close();
    }

}
