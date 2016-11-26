package org.deeplearning4j.examples.dataExamples;

import com.google.common.annotations.VisibleForTesting;
import org.datavec.api.records.reader.RecordReader;
import org.datavec.api.records.reader.impl.csv.CSVRecordReader;
import org.datavec.api.split.FileSplit;
import org.datavec.api.util.ClassPathResource;
import org.deeplearning4j.datasets.datavec.RecordReaderDataSetIterator;
import org.deeplearning4j.eval.Evaluation;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.optimize.listeners.ScoreIterationListener;
import org.json.JSONObject;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.SplitTestAndTrain;
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator;
import org.nd4j.linalg.dataset.api.preprocessor.DataNormalization;
import org.nd4j.linalg.dataset.api.preprocessor.NormalizerStandardize;
import org.nd4j.linalg.lossfunctions.LossFunctions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.*;
import java.io.IOException;
import java.io.Writer;
import org.mortbay.util.ajax.JSON;

/**
 * @author Adam Gibson
 */
public class CSVExample {

    private static Logger log = LoggerFactory.getLogger(CSVExample.class);

    static MultiLayerConfiguration conf = new MultiLayerConfiguration();
    static DataSet testDataForRunModel = new DataSet();

    public static void main(String[] args) throws Exception {

        //First: get the dataset using the record reader. CSVRecordReader handles loading/parsing
        RecordReader recordReader=getInputDataSet();
        //Second: the RecordReaderDataSetIterator handles conversion to DataSet objects, ready for use in neural network
        SplitTestAndTrain testAndTrain = getDataSetObjects(8,2,8850, recordReader);
        //build the model
        MultiLayerConfiguration conf = buildModel(8,2,100,123);
        //get training data for fitting the model
        DataSet dataSet = getTrainingDataSet(testAndTrain);
        //run the model, fit and evaluate the model
        runModel(conf,dataSet);



    }

    //First: get the dataset using the record reader. CSVRecordReader handles loading/parsing
    public static RecordReader getInputDataSet() throws Exception {

            File inputFile = new File(new String());
            int numLinesToSkip = 0;
            String delimiter = ",";
            RecordReader recordReader = new CSVRecordReader(numLinesToSkip, delimiter);
            FileSplit fileSplit = new FileSplit(new ClassPathResource("irisoutput.txt").getFile());
            recordReader.initialize(fileSplit);
            return recordReader;
    }

    //Second: the RecordReaderDataSetIterator handles conversion to DataSet objects, ready for use in neural network
    public static SplitTestAndTrain getDataSetObjects(int numberOfLabelIndex, int numClassesPresent, int batchSizePresent, RecordReader inputRecordReader){
        int labelIndex=0;
        int numClasses=0;
        int batchSize = 0;
        labelIndex =numberOfLabelIndex;     //5 values in each row of the iris.txt CSV: 4 input features followed by an integer label (class) index. Labels are the 5th value (index 4) in each row
        numClasses =numClassesPresent;     //3 classes (types of iris flowers) in the iris data set. Classes have integer values 0, 1 or 2
        batchSize = batchSizePresent;    //Iris data set: 150 examples total. We are loading all of them into one DataSet (not recommended for large data sets)

        DataSetIterator iterator = new RecordReaderDataSetIterator(inputRecordReader, batchSize, labelIndex, numClasses);
        DataSet allData = iterator.next();
        allData.shuffle();
        SplitTestAndTrain testAndTrain = allData.splitTestAndTrain(0.8);  //Use 65% of data for training

        return testAndTrain;
    }


    public static DataSet getTrainingDataSet(SplitTestAndTrain testAndTrain) throws IOException {
        DataSet testDataForRunModel = testAndTrain.getTest();
        DataSet trainingData = testAndTrain.getTrain();

        //We need to normalize our data. We'll use NormalizeStandardize (which gives us mean 0, unit variance):
        DataNormalization normalizer = new NormalizerStandardize();
        normalizer.fit(trainingData);           //Collect the statistics (mean/stdev) from the training data. This does not modify the input data
        normalizer.transform(trainingData);     //Apply normalization to the training data
        normalizer.transform(testDataForRunModel);         //Apply normalization to the test data. This is using statistics calculated from the *training* set

        //saving the dataset
        File fileDataSet = new File("/Users/avakil/dataSetNew.txt");
        fileDataSet.createNewFile();
        testDataForRunModel.save(fileDataSet);

        return trainingData;

    }

    public static MultiLayerConfiguration buildModel( int numbInputs, int opNum, int iteration, long seeds) throws IOException {
        int numInputs = 0;
        int outputNum = 0;          //3
        int iterations = 0;
        long seed = 0l;

        numInputs=numbInputs;
        outputNum=opNum;
        iterations=iteration;
        seed=seeds;

        log.info("Build model....");
         conf = new NeuralNetConfiguration.Builder()
            .seed(seed)
            .iterations(iterations)
            .activation("tanh")
            .weightInit(WeightInit.XAVIER)
            .learningRate(0.1)
            .regularization(true).l2(1e-4)
            .list()
            .layer(0, new DenseLayer.Builder().nIn(numInputs).nOut(3)
                .build())
            .layer(1, new DenseLayer.Builder().nIn(3).nOut(3)
                .build())
            .layer(2, new OutputLayer.Builder(LossFunctions.LossFunction.NEGATIVELOGLIKELIHOOD)
                .activation("softmax")
                .nIn(3).nOut(outputNum).build())
            .backprop(true).pretrain(false)
            .build();

        //saving the configuration
        File fileConf = new File("/Users/avakil/MultiLayerConfiguration.txt");
        fileConf.createNewFile();
        FileWriter file = new FileWriter(fileConf.getAbsoluteFile(),true);
        BufferedWriter bw = new BufferedWriter(file);
        bw.write(conf.toJson());
        bw.close();

        return conf;

    }

    public static void runModel (MultiLayerConfiguration configuration,DataSet trainingData){

        MultiLayerNetwork model = new MultiLayerNetwork(configuration);
        model.init();
        model.setListeners(new ScoreIterationListener(100));
        model.fit(trainingData);
        //evaluate the model on the test set
        Evaluation eval = new Evaluation(2);
        INDArray output = model.output(trainingData.getFeatureMatrix());
       for ( int i =0; i<output.length();i++) {
            System.out.println(" Actual value: "+trainingData.getLabels().getDouble(i)+" Prediction: " + output.getDouble(i));
        }
        eval.eval(trainingData.getLabels(), output);
        log.info(eval.stats());
   /*     -model.serialize()
            -serparate file derealize it; pass each record input
            -prediction = model.output( input )
                -print this*/
    }

    //getter for DL model's configuration
    public MultiLayerConfiguration getConf(){
        return conf;
    }

    //getter for dataset
    public DataSet getDataSet(){
        DataNormalization normalizer = new NormalizerStandardize();
        return testDataForRunModel;
    }


}

