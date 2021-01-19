package Producer;

import com.sun.xml.internal.ws.util.Pool;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

public class ConsumerClass {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerClass.class);

        //* https://kafka.apache.org/documentation/#consumerconfigs

        String bootstrapServer = "127.0.0.1:9092";

        String topic = "first_topic";

        //* Se trocar o nome do Grupo, você acaba se conectando ao tópico novamente do começo, praticamente restarta
        //* a aplicação
        String groupName = "MyFirstGroup";

        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);

        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupName);

        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        kafkaConsumer.subscribe(Arrays.asList(topic));

        //*Laço para testar a recepção de mensagens
        while (true){

            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String, String> record : consumerRecords){
                logger.info("Chave da Mensagem: " + record.key());
                logger.info("Conteúdo da Mensagem: " + record.value());
            }
        }

    }

}
