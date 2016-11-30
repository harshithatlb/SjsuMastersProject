package org.deeplearning4j.examples.dataExamples;

/**
 * Created by avakil on 11/19/16.
 */


        import org.apache.kafka.clients.consumer.KafkaConsumer;
        import org.apache.kafka.common.serialization.StringDeserializer;
        import org.apache.kafka.clients.consumer.*;
        import java.util.Properties;

        import org.apache.kafka.clients.consumer.ConsumerRecords;
        import org.apache.kafka.clients.consumer.ConsumerRecord;
        import java.util.*;

public final class ConsumerLoop {


        public static void main(String[] args) throws Exception {
            if(args.length < 2){
                System.out.println("Usage: consumer <topic> <groupname>");
                return;
            }

            String topic = args[0].toString();
            String group = args[1].toString();
            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");
            props.put("group.id", group);
            props.put("enable.auto.commit", "true");
            props.put("auto.commit.interval.ms", "1000");
            props.put("session.timeout.ms", "30000");
            props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
            // props.put("key.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");
            props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

            consumer.subscribe(Arrays.asList(topic));
            System.out.println("Subscribed to topic " + topic);
            int i = 0;

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records)
                    System.out.printf("offset = %d, key = %s, value = %s\n",
                        record.offset(), record.key(), record.value());
            }
        }

}
