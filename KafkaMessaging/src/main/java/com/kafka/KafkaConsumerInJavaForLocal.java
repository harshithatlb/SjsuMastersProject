//This class consumes the streaming  data - ie with record id

package com.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.datavec.api.records.reader.RecordReader;
import org.datavec.api.records.reader.impl.csv.CSVRecordReader;
import org.datavec.api.split.FileSplit;
import org.deeplearning4j.datasets.datavec.RecordReaderDataSetIterator;
import org.deeplearning4j.examples.dataExamples.DdosDL4JRun;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.SplitTestAndTrain;
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator;
import com.kafka.ProducerKafkaToES;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.Properties;

/**
 * Created by avakil on 11/29/16.
 *
 *
 */
public class KafkaConsumerInJavaForLocal {


    static String topicName = "";
    static String topicNameToEC = "";

    public static void main(String[] args) throws Exception {

        //prediction for the record received by consumer
        String predict = "";

       if (args.length == 0) {
            System.out.println("Enter topic name");
            return;
        }
        //Kafka consumer configuration settings
        topicName = args[0].toString();
        topicNameToEC =  args[1].toString();

        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");
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


        BufferedReader reader = new BufferedReader(new FileReader("/Users/avakil/dl4j-examples/model.txt"));
        String content;
        String output = "";
        while ( (content = reader.readLine()) != null )
            output = output.concat(content);
        System.out.println("output: "+output);

        //One time initialization of MultiLayerConfiguration
        MultiLayerConfiguration config = MultiLayerConfiguration.fromJson(output);
        // System.out.println("config: "+config);
        MultiLayerNetwork ml = new MultiLayerNetwork(config);
        ml.init();

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);

            //  String record ="AVfl3EseAHODrg4HFrEC|9,4005,564312763,1,1,1,0,1";


         if (records != null){
               for (ConsumerRecord<String, String> record : records) {
                    String ignoreRecord = "0,0000,00000000,0,0,0,0,0,0\n";


                 //    String[] values = record.split("|");
                    String[] values = record.value().split("|");

                    System.out.println("values[0]: "+values[0]);

                    String extractedRecord = "";
                   String recordId = "";
                    for ( int i=0; i<values.length;i++){
                        if (values[i].equals("|")) {
                            extractedRecord = record.value().substring(i);
                            recordId = record.value().substring(0,i-1);
                            break;
                        }

                    }

            System.out.println("extractedRecord: "+extractedRecord);

                    File file = new File("./data.txt");
                    FileWriter fw = new FileWriter(file.getAbsoluteFile());
                    BufferedWriter bw = new BufferedWriter(fw);
                    bw.write(ignoreRecord);
                    bw.write(extractedRecord+",0");
                    bw.close();



                    RecordReader recordReader = getInputDataSet();
                    SplitTestAndTrain testData = getDataSetObjects(8, 2, 2, recordReader);
                    DataSet dataSet = testData.getTest();


                    System.out.println("Dataset: " + dataSet);
                   DLKafkaProducer producer = new DLKafkaProducer(topicNameToEC);



                    predict = DdosDL4JRun.runModel(dataSet, ml);

                    Double predictScore = Double.parseDouble(predict);

                    if (predictScore >= 0.5){
                        predict = String.valueOf(1);
                    }else
                        predict = String.valueOf(0);

                   System.out.println("Predict sent to producer: "+producer.topicName);
                   producer.produceMessage(recordId, predict);





                    //  print the offset,key and value for the consumer records.
                    System.out.printf("offset = %d, key = %s, value = %s, recordid=%s, prediction=%s\n",
                            record.offset(), record.key(), extractedRecord,recordId,predict);
                }

            }

        }
    }
    public static RecordReader getInputDataSet() throws Exception {
        new File(new String());
        byte numLinesToSkip = 0;
        String delimiter = ",";
        CSVRecordReader recordReader = new CSVRecordReader(numLinesToSkip, delimiter);
        FileSplit fileSplit = new FileSplit(new File("./data.txt"));
        recordReader.initialize(fileSplit);
        return recordReader;
    }

    public static SplitTestAndTrain getDataSetObjects(int numberOfLabelIndex, int numClassesPresent, int batchSizePresent, RecordReader inputRecordReader){
        int labelIndex=0;
        int numClasses=0;
        int batchSize = 0;
        labelIndex =numberOfLabelIndex;     //8 input features followed by an integer label (class) index. Labels are the 9th value (index 8) in each row
        numClasses =numClassesPresent;     //2 classes (attack/not attack) . Classes have integer values 0 or 1
        batchSize = batchSizePresent;    //Iris data set: 150 examples total. We are loading all of them into one DataSet (not recommended for large data sets)

        DataSetIterator iterator = new RecordReaderDataSetIterator(inputRecordReader, batchSize, labelIndex, numClasses);
        DataSet allData = iterator.next();
        SplitTestAndTrain testData = allData.splitTestAndTrain(0.5);  //Use 65% of data for training
        System.out.println("testData: " + testData.getTest());
        return testData;
    }

}



//java -cp messaging-1.0-SNAPSHOT-bin.jar com.kafka.KafkaConsumerInJavaForLocal test14
//java -cp messaging-1.0-SNAPSHOT-bin.jar com.kafka.KafkaProducerInJavaForLocal test14
