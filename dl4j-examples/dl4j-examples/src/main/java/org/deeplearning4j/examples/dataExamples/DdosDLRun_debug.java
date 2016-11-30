package org.deeplearning4j.examples.dataExamples;

import org.deeplearning4j.eval.Evaluation;
import org.deeplearning4j.nn.api.Layer;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.optimize.listeners.ScoreIterationListener;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by avakil on 11/27/16.
 */
public class DdosDLRun_debug {

    static MultiLayerNetwork model;
    public DdosDLRun_debug() throws FileNotFoundException {
    }

    public static String runModel(DataSet dataSet, MultiLayerNetwork  givenModel) throws IOException {
        DataSet ds = dataSet;
         model = givenModel;
        String prediction = "";


        try{

            //get the dataset model
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
        BufferedReader reader = new BufferedReader(new FileReader("./model.txt"));
        MultiLayerConfiguration config = new MultiLayerConfiguration();
        String content;
        String output = "";
        while ( (content = reader.readLine()) != null )
            output = output.concat(content);
        System.out.println("output: "+output);
        config = config.fromJson(output);
        System.out.println("config: "+config);
        MultiLayerNetwork ml = new MultiLayerNetwork(config);
        ml.init();


        //calls run model on test data
        String prediction = runModel(dataSet,ml);
    }
}
