import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ProducerApp {

    public static void main(String[] args){
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG,"client_producer_1");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        Random random = new Random();
        Executors
                .newScheduledThreadPool(1)
                .scheduleAtFixedRate(() -> {
                    String key = String.valueOf(random.nextInt(1000));
                    String value = String.valueOf(random.nextDouble()*90000);
                    kafkaProducer.send(new ProducerRecord<String, String>("test4",key, value),(metadata, ex)->{
                        System.out.printf("Sending message => "+ value+" Partition => "+ metadata.partition()+" => " +metadata.offset() + "\n");
                });
            },1000,1000, TimeUnit.MILLISECONDS);
    }
}
