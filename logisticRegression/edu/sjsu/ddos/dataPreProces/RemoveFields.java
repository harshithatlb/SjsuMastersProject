package edu.sjsu.ddos.dataPreProces;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;

public class RemoveFields {
	public static final long serialVersionUID = 1L;

	public static void main(String[] args) {
		
		JavaSparkContext sc = new JavaSparkContext(
				new SparkConf().setAppName("FilterTCP"));
		JavaRDD<String> textFile = sc.textFile(args[0]);
		//sPort,dPort,protocol,packets,bytes,flags,duration
		//protocol,packets,bytes,duration -- New
		Function2 removeHeader= new Function2<Integer, Iterator<String>, Iterator<String>>(){
		   // @Override
		    public Iterator<String> call(Integer ind, Iterator<String> iterator) throws Exception {
		        if(ind==0 && iterator.hasNext()){
		            iterator.next();
		            return iterator;
		        }else
		            return iterator;
		    }
		};
		JavaRDD<String> inputRdd = textFile.mapPartitionsWithIndex(removeHeader, false);
		
		System.out.println("Removed Header");
		JavaRDD<String> filterDataSet = inputRdd
				.filter(new Function<String, Boolean>() {
					public Boolean call(String s) {
						String[] columns = new String[4];
						columns = s.split(",");
						//System.out.println("@@@@@@@@@@@@@@@@@@@ columns[0] "+columns[0]);
						try {
						if (Integer.parseInt(columns[0]) == 6) {
							return true;
						} else
							return false;
						}
						catch(NumberFormatException nfe) {
					        return false;
						}
					}
				});
		System.out.println("Filtered TCP");
		JavaRDD<String> newDataSet = filterDataSet
				.map(new Function<String, String>() {
					public String call(String s) {
						String[] columns = new String[4];
						columns = s.split(",");
						//Add 0 for normal data set and 1 for attack
						String newLine = "0,"+ columns[1] + "," + columns[2] + ","
								+ columns[3];
						return newLine;
				}
				});
		System.out.println("Removed Protocol Column");
		newDataSet.saveAsTextFile(args[1]);
		
		
	}
}
