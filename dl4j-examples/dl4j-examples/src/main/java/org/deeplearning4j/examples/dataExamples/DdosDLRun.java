package org.deeplearning4j.examples.dataExamples;


/**
 * Created by avakil on 10/13/16.
 */



import org.deeplearning4j.eval.Evaluation;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.optimize.listeners.ScoreIterationListener;
import org.jfree.data.general.Dataset;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.SplitTestAndTrain;

import java.io.File;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import org.nd4j.linalg.dataset.DataSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DdosDLRun {
    private static Logger log = LoggerFactory.getLogger(DdosDLRun.class);


    public DdosDLRun() throws FileNotFoundException {
    }

    public static String runModel(DataSet dataSet, MultiLayerConfiguration config) throws IOException {
        DataSet ds = dataSet;
        MultiLayerConfiguration conf = config;
        String prediction = "";


      try{

             //get the dataset model
            MultiLayerNetwork model = new MultiLayerNetwork(conf);
            model.init();
            model.setListeners(new ScoreIterationListener(100));
            model.fit(ds);
            //evaluate the model on the test set
            Evaluation eval = new Evaluation(2);
            INDArray output = model.output(ds.getFeatureMatrix());
            for ( int i =0; i<output.length();i++) {
                System.out.println(" Actual value: "+ model.getLabels().getDouble(i)+" Prediction: " + output.getDouble(i));
                //returning the prediction for DLKafkaProducer
                prediction = String.valueOf(output.getDouble(i));
            }
            eval.eval(ds.getLabels(), output);
            log.info(eval.stats());

        }catch(Exception e){
            System.out.println("Error while running the model");
            e.printStackTrace();
        }
        return prediction;
    }

    public static void main(String[] arg) throws IOException {

        //reads the test dataset
        FileInputStream fileInputStream = new FileInputStream("./dataSetNew.txt");
        DataSet dataSet = new DataSet();
        dataSet.load(fileInputStream);
        System.out.println(dataSet.toString());

        //reads the model build
        BufferedReader reader = new BufferedReader(new FileReader("/Users/avakil/MultiLayerConfiguration.txt"));
        MultiLayerConfiguration config = new MultiLayerConfiguration();
        String content;
        String output = "";
        while ( (content = reader.readLine()) != null )
            output = output.concat(content);
        System.out.println("content: "+output);
        config = config.fromJson(output);
        System.out.println("config: "+config.toString());

        //calls run model on test data
        String prediction = runModel(dataSet,config);
    }
}



//will have to store the model build in a file and then call the run method on it
