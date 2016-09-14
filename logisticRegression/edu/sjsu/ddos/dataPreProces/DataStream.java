package edu.sjsu.ddos.dataPreProces;
import org.apache.spark.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;

import scala.Tuple2;
import scala.tools.nsc.interpreter.Results;
public class DataStream {
	
	public static void main(String[] args) {
			SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("Get NetFlow");
			JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
			// Create a DStream that will connect to hostname:port, like localhost:9999
			JavaReceiverInputDStream<String> lines = jssc.socketTextStream(args[1], Integer.parseInt(args[2]));
			System.out.println(lines);
			JavaDStream<Tuple2<String, LabeledPoint>> newDataSet = lines.map(new Function<String,Tuple2<String, LabeledPoint>>() {
				public Tuple2<String, LabeledPoint> call(String s) {
					String[] parts = new String[11];
					parts = s.split(",");
					 double[] points = new double[3];
					//Add 0 for normal data set and 1 for attack
					
			    	     points[0] = Double.valueOf(parts[8]);
			    	     points[1] = Double.valueOf(parts[9]);
			    	     points[2] = Double.valueOf(parts[10]);
			    	     String key = parts[0]+","+parts[1]+","+parts[2]+","+parts[3]+","+parts[4]+","+parts[5]+","+parts[6]+","+parts[7];
			    	 return new Tuple2<String, LabeledPoint>(key, 
			    			 new LabeledPoint(Double.valueOf(parts[0]), Vectors.dense(points)));
			    	
			}
			});

JavaDStream<LabeledPoint> labeledSet = newDataSet
		.map(new Function<Tuple2<String, LabeledPoint>,LabeledPoint>() {
			public  LabeledPoint call(Tuple2<String, LabeledPoint> z) {
				 return z._2();
		    	
		}
		});
    

final LogisticRegressionModel sameModel = LogisticRegressionModel.load(jssc.sc().sc(), args[0]);
// sameModel.setThreshold(0.5);

JavaDStream<Tuple2<String, Double>> resultSet = newDataSet
		.map(new Function<Tuple2<String, LabeledPoint>,Tuple2<String, Double>>() {
			public Tuple2<String, Double> call(Tuple2<String, LabeledPoint> z) {
				return new Tuple2<String, Double>(z._1(), sameModel.predict(z._2().features()));
			}
		});

resultSet. print();

jssc.start();
jssc.awaitTermination();

}
}
