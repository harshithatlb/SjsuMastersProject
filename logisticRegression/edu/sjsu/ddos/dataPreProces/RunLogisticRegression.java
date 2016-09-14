package edu.sjsu.ddos.dataPreProces;
import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
public class RunLogisticRegression {
	public static final long serialVersionUID = 1L;
	public static void main(String[] args) {
		JavaSparkContext sc = new JavaSparkContext(
				new SparkConf().setAppName("RunLogisticRegression"));
	    JavaRDD<String> textFile=sc.textFile(args[0]);
	  
	    
	    JavaRDD<Tuple2<String, LabeledPoint>> newDataSet = textFile
				.map(new Function<String,Tuple2<String, LabeledPoint>>() {
					public Tuple2<String, LabeledPoint> call(String s) {
						String[] parts = new String[4];
						parts = s.split(",");
						 double[] points = new double[parts.length - 1];
						//Add 0 for normal data set and 1 for attack
						for (int i = 1; i < (parts.length); i++) {
				    	     points[i-1] = Double.valueOf(parts[i]);
				    	 }
				    	 return new Tuple2<String, LabeledPoint>(parts[0], 
				    			 new LabeledPoint(Double.valueOf(parts[0]), Vectors.dense(points)));
				    	
				}
				});
	    
	    JavaRDD<LabeledPoint> labeledSet = newDataSet
				.map(new Function<Tuple2<String, LabeledPoint>,LabeledPoint>() {
					public  LabeledPoint call(Tuple2<String, LabeledPoint> z) {
						 return z._2();
				    	
				}
				});
	        
	      
	    final LogisticRegressionModel sameModel = LogisticRegressionModel.load(sc.sc(), args[1]);
	    sameModel.setThreshold(0.05);
	    
	    JavaRDD<Tuple2<String, Double>> results = newDataSet
				.map(new Function<Tuple2<String, LabeledPoint>,Tuple2<String, Double>>() {
					public Tuple2<String, Double> call(Tuple2<String, LabeledPoint> z) {
						return new Tuple2<String, Double>(z._1(), sameModel.predict(z._2().features()));
					}
				});
	    
	   
	    results.foreach(new VoidFunction<Tuple2<String, Double>>() 
	    		{ public void call(Tuple2<String, Double> t) throws Exception { System.out.println(t._1() + " " + t._2()); } });
	    
	  }
}
