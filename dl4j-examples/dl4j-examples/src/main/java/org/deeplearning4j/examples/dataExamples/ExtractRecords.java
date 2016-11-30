package org.deeplearning4j.examples.dataExamples;

import org.datavec.api.records.reader.RecordReader;
import org.datavec.api.records.reader.impl.csv.CSVRecordReader;
import org.datavec.api.split.FileSplit;
import org.datavec.api.util.ClassPathResource;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.OutputStreamWriter;
import java.util.*;

/**
 * Created by avakil on 11/12/16.
 */
public class ExtractRecords {

    private static Integer type = 0;
    private static Integer SIP = 1;
    private static Integer DIP = 2;
    private static Integer Sport = 3;
    private static Integer Dport = 4;
    private static Integer StartTime = 5;
    private static Integer EndTime = 6;
    private static Integer flags = 7;
    private static Integer packets = 8;
    private static Integer bytes = 9;
    private static Integer duration = 10;


    public static FileSplit fileSplit;

    //First: get the dataset using the record reader. CSVRecordReader handles loading/parsing
    public static RecordReader getInputDataSet() throws Exception {

        File inputFile = new File(new String());
        int numLinesToSkip = 0;
        String delimiter = ",";
        RecordReader recordReader = new CSVRecordReader(numLinesToSkip, delimiter);
        fileSplit = new FileSplit(new ClassPathResource("iris.txt").getFile());
        recordReader.initialize(fileSplit);
        return recordReader;
    }

    public static void extractRecords() throws Exception {
        ArrayList<String> outputTokens = new ArrayList<String>();

        try {
            // Open the file that is the first
            // command line parameter
            FileInputStream fstream = new FileInputStream("/Users/avakil/dl4j-examples/dl4j-examples/src/main/resources/iris.txt");
            BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
            String strLine;


            FileOutputStream fileOutputStream = new FileOutputStream("/Users/avakil/dl4j-examples/dl4j-examples/src/main/resources/irisoutput.txt",true);
            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileOutputStream));


            // Read File Line By Line
            while ((strLine = br.readLine()) != null) {
                strLine = strLine.trim();

                if ((strLine.length() != 0) && (strLine.charAt(0) != '#')) {
                    String[] outputSplit = strLine.split(",");
                    String[] outputSplitFlags = new String[5];
                    if(outputSplit[7].contains("A"))
                        outputSplitFlags[0] = "1";
                    else
                        outputSplitFlags[0] = "0";
                    if(outputSplit[7].contains("F"))
                        outputSplitFlags[1] = "1";
                    else
                        outputSplitFlags[1] = "0";
                    if(outputSplit[7].contains("P"))
                        outputSplitFlags[2] = "1";
                    else
                        outputSplitFlags[2] = "0";
                    if(outputSplit[7].contains("R"))
                        outputSplitFlags[3] = "1";
                    else
                        outputSplitFlags[3] = "0";
                    if(outputSplit[7].contains("S"))
                        outputSplitFlags[4] = "1";
                    else
                        outputSplitFlags[4] = "0";

                    bufferedWriter.write(outputSplit[0]+","+outputSplit[8]+","+outputSplit[9]+","+outputSplit[10]+","+outputSplitFlags[0]+","+outputSplitFlags[1]+","+outputSplitFlags[2]+","+outputSplitFlags[3]+","+outputSplitFlags[4]+'\n');
                }


            }

            for (String s : outputTokens) {
                System.out.println(s);
            }

            // Close the input stream
            br.close();
            bufferedWriter.close();
        } catch (Exception e) {// Catch exception if any
            System.err.println("Error: " + e.getMessage());
        }
    }

        public static void main(String[] arg) throws Exception{
            extractRecords();
        }




}
