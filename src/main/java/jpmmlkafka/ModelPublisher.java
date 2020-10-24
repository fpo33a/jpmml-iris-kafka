/*

kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic model
kafka-console-consumer.bat --bootstrap-server localhost:9092 --from-beginning --topic model

 */

package jpmmlkafka;

import java.io.File;
import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ModelPublisher {

    public static void main(String args[]) {
        ModelPublisher mp = new ModelPublisher();
        mp.publishModel("model", "iris_rf.pmml");
    }

    public void publishModel(String topicName, String fileName) {
        try {

            // create instance for properties to access producer configs
            Properties props = new Properties();

            props.put("bootstrap.servers", "localhost:9092");
            props.put("acks", "all");
            props.put("retries", 0);
            props.put("batch.size", 16384);
            props.put("linger.ms", 1);
            props.put("buffer.memory", 33554432);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            Producer<String, String> producer = new KafkaProducer<String, String>(props);

            // read pmml file from resource and push it into topic
            File pmmlFile = new File(JpmmlModelIris.class.getResource(fileName).getPath());
            String pmmlString = new Scanner(pmmlFile).useDelimiter("\\Z").next();
            producer.send(new ProducerRecord<String, String>(topicName, fileName, pmmlString));
            System.out.println("Model published");
            producer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
