package com.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by avakil on 12/6/16.
 */
public class ConsumerKafkaToES {



    public static void main(String[] args) throws Exception {

        //prediction for the record received by consumer
        String topicName = "";

        if (args.length == 0) {
            System.out.println("Enter topic name");
            return;
        }
        //Kafka consumer configuration settings
        topicName = args[0].toString();



        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9093");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer
                <String, String>(props);

        //Kafka Consumer subscribes list of topics here
        consumer.subscribe(Arrays.asList(topicName));

        //print the topic name
        System.out.println("Subscribed to topic " + topicName);
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            if (records != null) {
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Record id: "+record.key()+" Prediction: "+record.value());
                }
            }
        }

    }
}
