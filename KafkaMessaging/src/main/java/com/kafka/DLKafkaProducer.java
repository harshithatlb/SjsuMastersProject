package com.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.deeplearning4j.examples.dataExamples.DdosDLRun;

import java.util.Properties;

/**
 * Created by avakil on 11/26/16.
 */
public class DLKafkaProducer {

    Producer<String, String> producer;
    String topicName = "";

    public DLKafkaProducer( String topicname){


        //Assign topicName to string variable
         topicName = topicname;

        // create instance for properties to access producer configs
        Properties props = new Properties();

        //Assign localhost id
        props.put("bootstrap.servers","localhost/:9092");

        //Set acknowledgements for producer requests.
        props.put("acks","all");

        //If the request fails, the producer can automatically retry,
        props.put("retries", 0);

        //Specify buffer size in config
        props.put("batch.size", 16384);

        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);

        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<String, String>(props);

    }

    public void produceMessage(String recordId,String prediction){
        producer.send(new ProducerRecord<String, String>(topicName,
                recordId, prediction));
        System.out.println("Message sent successfully for record id:"+recordId+" and prediction: "+prediction);
        producer.close();
    }

}
