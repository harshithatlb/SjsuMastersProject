package com.kafka;

/**
 * Created by avakil on 11/21/16.
 */


        import java.util.HashMap;
        import java.util.HashSet;
        import java.util.Arrays;
        import java.util.Iterator;
        import java.util.List;
        import java.util.Map;
        import java.util.Set;
        import java.util.regex.Pattern;

        import scala.Tuple2;
        import kafka.serializer.StringDecoder;


        import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
        import org.apache.spark.SparkConf;
        import org.apache.spark.api.java.JavaPairRDD;
        import org.apache.spark.api.java.JavaRDD;
        import org.apache.spark.api.java.function.*;
        import org.apache.spark.mllib.clustering.GaussianMixtureModel;
        import org.apache.spark.mllib.linalg.Vector;
        import org.apache.spark.mllib.linalg.Vectors;
        import org.apache.spark.streaming.api.java.*;
        import org.apache.spark.streaming.kafka.KafkaUtils;
        import org.apache.spark.streaming.Durations;


/**
 * Consumes messages from one or more topics in Kafka and does wordcount. Usage:
 * JavaDirectKafkaWordCount <brokers> <topics> <brokers> is a list of one or
 * more Kafka brokers <topics> is a list of one or more kafka topics to consume
 * from
 *
 * Example: $ bin/run-example streaming.JavaDirectKafkaWordCount
 * broker1-host:port,broker2-host:port \ topic1,topic2
 */
public final class sensorModel {

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err
                    .println("Usage: JavaDirectKafkaWordCount <brokers> <topics>\n"
                            + "  <brokers> is a list of one or more Kafka brokers\n"
                            + "  <topics> is a list of one or more kafka topics to consume from\n\n"
                            + "  GMM MOdel Spark model used for prediction\n\n");
            System.exit(1);
        }

        String brokers = args[0];
        String topics = args[1];

        SparkConf sparkConf = new SparkConf()
                .setAppName("JavaDirectKafkaWordCount");

        sparkConf.set("es.nodes", "localhost");
        sparkConf.set("es.port", "9200");

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,
                Durations.seconds(6));

        Set<String> topicsSet = new HashSet<String>(Arrays.asList(topics
                .split(",")));
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", brokers);

        // Create direct kafka stream with brokers and topics
        JavaPairInputDStream<String, String> messages = KafkaUtils
                .createDirectStream(jssc, String.class, String.class,
                        StringDecoder.class, StringDecoder.class, kafkaParams,
                        topicsSet);


        JavaDStream<String> lines = messages
                .map(new Function<Tuple2<String, String>, String>() {
                    public String call(Tuple2<String, String> tuple2) {
                        System.out.println(" @@@@ " + tuple2._1() + " "
                                + tuple2._2());
                        return tuple2._2();
                    }
                });


        JavaPairDStream<String, Vector> newDataSet = lines
                .mapToPair(new PairFunction<String, String, Vector>() {
                    public Tuple2<String, Vector> call(String s) {
                        String[] parts = new String[2];
                        parts = s.split(",");

                        double vibration = Double.parseDouble(parts[1]);
                        String key = s;
                        return new Tuple2<String, Vector>(key, Vectors
                                .dense(vibration));

                    }
                });

        // Cluster the data into 3 classes using GaussianMixture
        final GaussianMixtureModel gmModel = GaussianMixtureModel.load(jssc
                .sparkContext().sc(), args[2]);

        JavaDStream<Map<String, String>> esResult = newDataSet
                .map(new Function<Tuple2<String, Vector>, Map<String, String>>() {
                    private static final long serialVersionUID = 6272424972267329328L;

                    public Map<String, String> call(Tuple2<String, Vector> z) {
                        Tuple2<String, Integer> results = new Tuple2<String, Integer>(
                                z._1(), gmModel.predict(z._2()));

                        Map<String, String> sensorMap = new HashMap<String, String>();
                        String[] parts = new String[2];
                        parts = results._1().split(",");
                        sensorMap.put("vibration", parts[1]);
                        sensorMap.put("time", parts[0]);
                        sensorMap.put("status", results._2().toString());
                        return sensorMap;
                    }
                });

        esResult.foreach(new Function<JavaRDD<Map<String, String>>, Void>() {
            private static final long serialVersionUID = 6272424972267329328L;

            public Void call(JavaRDD<Map<String, String>> rdd) throws Exception {
                JavaEsSpark.saveToEs(rdd, "/sensorstatus/sensorstatusmapping");
                return null;
            }
        });

        jssc.start();
        jssc.awaitTermination();
    }

}
