package Producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerClass {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerClass.class);

        //* https://kafka.apache.org/documentation/#producerconfigs

        String bootstrapServer = "127.0.0.1:9092";

        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);

        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "Mensagem Teste!");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e == null){
                    logger.info("Registrando Dados da Mensagem Enviada... \n" +
                    "Tópico: " + recordMetadata.topic() +
                    "\nOffset: " + recordMetadata.offset());
                }
                else
                    logger.error("Uma Exception Ocorreu...", e);
                }
            });
        producer.flush();
        producer.close();
    }

}
