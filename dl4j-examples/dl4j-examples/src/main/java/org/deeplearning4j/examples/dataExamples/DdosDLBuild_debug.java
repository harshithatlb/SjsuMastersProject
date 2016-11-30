package org.deeplearning4j.examples.dataExamples;

import org.datavec.api.conf.Configuration;
import org.datavec.api.records.reader.RecordReader;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.optimize.listeners.ScoreIterationListener;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.SplitTestAndTrain;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.nio.charset.Charset;

/**
 * Created by avakil on 11/27/16.
 */
public class DdosDLBuild_debug {
    public static DataSet dataSet;
    public static MultiLayerConfiguration conf;
    protected DataOutputStream out;
    public static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");
    protected Charset encoding;
    protected File writeTo;
    protected Configuration config;
    private boolean append;

    public static void buildModel(){ //throws IOException{

        CSVExample csvExample = new CSVExample();
        try {
            //First: get the dataset using the record reader. CSVRecordReader handles loading/parsing
            RecordReader recordReader=csvExample.getInputDataSet();
            //Second: the RecordReaderDataSetIterator handles conversion to DataSet objects, ready for use in neural network
            SplitTestAndTrain testAndTrain = csvExample.getDataSetObjects(8,2,6118, recordReader);
            //build the model
            conf = csvExample.buildModel(8,2,100,123);

            //train the model
            MultiLayerNetwork model = new MultiLayerNetwork(conf);
            model.init();
            model.setListeners(new ScoreIterationListener(100));
            model.fit(testAndTrain.getTrain());

            File fileConf = new File("./model.txt");
            fileConf.createNewFile();
            FileWriter file = new FileWriter(fileConf.getAbsoluteFile(),true);
            BufferedWriter bw = new BufferedWriter(file);
            bw.write(conf.toString());
          //  bw.write(String.valueOf(model));
            bw.close();

            dataSet = csvExample.getTrainingDataSet(testAndTrain);
            File fileDataSet = new File("./dataSetNew.txt");
            fileDataSet.createNewFile();
            dataSet.save(fileDataSet);



            System.out.println("Conffff: "+conf);
            System.out.println("Dataset: "+dataSet);
            //   FileRecordWriter fileRecordWriter = new FileRecordWriter(new File("Conf.txt"));



        } catch(Exception e) {
            System.out.println("Error while building the model");
            e.printStackTrace();
        }
    }


    public static void main(String arg[]){
        buildModel();
    }
}
