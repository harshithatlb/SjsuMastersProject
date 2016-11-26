package org.deeplearning4j.examples.dataExamples;

import org.apache.spark.api.java.JavaSparkContext;
import org.datavec.api.conf.Configuration;
import org.datavec.api.records.reader.RecordReader;
import org.datavec.api.records.writer.RecordWriter;
import org.datavec.api.records.writer.impl.FileRecordWriter;
import org.datavec.api.writable.Text;
import org.datavec.api.writable.Writable;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.SplitTestAndTrain;

import org.apache.spark.SparkConf;
import org.nd4j.linalg.dataset.api.preprocessor.DataNormalization;
import org.nd4j.linalg.dataset.api.preprocessor.NormalizerStandardize;
import org.spark_project.guava.io.FileWriteMode;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.File;
import java.nio.charset.Charset;
import java.util.Collection;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.Writer;

/**
 * Created by avakil on 10/8/16.
 */
public class DdosDLBuild {

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
            SplitTestAndTrain testAndTrain = csvExample.getDataSetObjects(4,2,6118, recordReader);
            //build the model
            conf = csvExample.buildModel(4,2,10000,123);
            //get training data for fitting the model

         //   FileRecordWriter writer = new FileRecordWriter();
         //   writer.write((Collection<Writable>) conf);
         //   writer.close();

            File fileConf = new File("/Users/avakil/confDataNew.txt");
            fileConf.createNewFile();
            FileWriter file = new FileWriter(fileConf.getAbsoluteFile(),true);
            BufferedWriter bw = new BufferedWriter(file);
            bw.write(conf.toJson());
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

   /* public void fileRecordWriter(){
        this.encoding = DEFAULT_CHARSET;
    }


    @Override
    public void write(Collection<Writable> collection) throws IOException {
        if(!collection.isEmpty()) {
            Text t = (Text)collection.iterator().next();
            t.write(this.out);
        }

    }

    @Override
    public void close() {
        try {
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void setConf(Configuration configuration) {
        this.config = configuration;
        this.writeTo = new File(config.get("org.datavec.api.records.writer.path", "outputDDOSConf.txt"));
        this.append = config.getBoolean("org.datavec.api.record.writer.append", true);

        try {
            this.out = new DataOutputStream(new FileOutputStream(this.writeTo, this.append));
        } catch (FileNotFoundException var3) {
            throw new RuntimeException(var3);
        }
    }

    @Override
    public Configuration getConf() {
        return null;
    }
*/
    public static void main(String arg[]){
        buildModel();
    }
}
