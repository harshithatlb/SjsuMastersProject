package edu.sjsu.ddos.dataPreProces;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

import scala.Tuple2;

public class RunNewLogisticRegression {
	public static final long serialVersionUID = 1L;
	public static void main(String[] args) {
		JavaSparkContext sc = new JavaSparkContext(
				new SparkConf().setAppName("RunLogisticRegression"));
	    JavaRDD<String> textFile=sc.textFile(args[0]);
	  
	    
	    //Label,sIP,dIP,sPort,dPort,sTime,eTime,flags,packets,bytes,duration
		  //	0,  1,  2,    3,   4,     5,     6,   7,    8,       9,  10
		  //Total 11
		  // For Prediction -- 7,8,9,10
		    
		    JavaRDD<Tuple2<String, LabeledPoint>> newDataSet = textFile
					.map(new Function<String,Tuple2<String, LabeledPoint>>() {
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
	    
	    JavaRDD<LabeledPoint> labeledSet = newDataSet
				.map(new Function<Tuple2<String, LabeledPoint>,LabeledPoint>() {
					public  LabeledPoint call(Tuple2<String, LabeledPoint> z) {
						 return z._2();
				    	
				}
				});
	        
	      
	    final LogisticRegressionModel sameModel = LogisticRegressionModel.load(sc.sc(), args[1]);
	   // sameModel.setThreshold(0.5);
	    
	    JavaRDD<Tuple2<String, Double>> results = newDataSet
				.map(new Function<Tuple2<String, LabeledPoint>,Tuple2<String, Double>>() {
					public Tuple2<String, Double> call(Tuple2<String, LabeledPoint> z) {
						return new Tuple2<String, Double>(z._1(), sameModel.predict(z._2().features()));
					}
				});
	    
	   // r//esults.col ;
	    results.foreach(new VoidFunction<Tuple2<String, Double>>() 
	    		{ public void call(Tuple2<String, Double> t) throws Exception {
	    			if (t._2()<0.5)
	    			System.out.println(t._1() + " " + t._2()); } });
	    
	  }
}
