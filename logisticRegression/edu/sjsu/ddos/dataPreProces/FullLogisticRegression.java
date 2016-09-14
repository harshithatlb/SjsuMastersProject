package edu.sjsu.ddos.dataPreProces;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;


public class FullLogisticRegression {
	
	public static void main(String[] args) {
		JavaSparkContext sc = new JavaSparkContext(
				new SparkConf().setAppName("LogisticRegression"));
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
	        
	    
	    	    
	    // Split initial RDD into two... [60% training data, 40% testing data].
	    JavaRDD<LabeledPoint>[] splits = labeledSet.randomSplit(new double[] {0.6, 0.4}, 11L);
	    JavaRDD<LabeledPoint> training = splits[0].cache();
	    JavaRDD<LabeledPoint> test = splits[1];

	    // Run training algorithm to build the model.
	    final LogisticRegressionModel model = new LogisticRegressionWithLBFGS().setNumClasses(2).setFeatureScaling(true).setIntercept(true)
	      .run(training.rdd());
	  
	 // Clear the prediction threshold so the model will return probabilities
	   model.clearThreshold();
	   // model.setThreshold(.1);

	 // Compute raw scores on the test set.
	    JavaRDD<Tuple2<Object, Object>> predictionAndLabels = test.map(
	      new Function<LabeledPoint, Tuple2<Object, Object>>() {
	        public Tuple2<Object, Object> call(LabeledPoint p) {
	          Double prediction = model.predict(p.features());
	          return new Tuple2<Object, Object>(prediction, p.label());
	        }
	      }
	    );

	   /* predictionAndLabels.foreach(new VoidFunction<Tuple2<Object, Object>>() 
	    		{ public void call(Tuple2<Object, Object> t) throws Exception { 
	    			//if ((Double)t._2()  > 0.0)
	    				System.out.println(t._1() + " " + t._2());
	    			} });*/
	
	    // Get evaluation metrics.
	    BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(predictionAndLabels.rdd());
	    
	    
	   // Precision by threshold
	    JavaRDD<Tuple2<Object, Object>> precision = metrics.precisionByThreshold().toJavaRDD();
	   /* System.out.println("Precision by threshold: " + precision.collect());

	    // Recall by threshold
	    JavaRDD<Tuple2<Object, Object>> recall = metrics.recallByThreshold().toJavaRDD();
	    System.out.println("Recall by threshold: " + recall.collect());

	    // F Score by threshold
	    JavaRDD<Tuple2<Object, Object>> f1Score = metrics.fMeasureByThreshold().toJavaRDD();
	    System.out.println("F1 Score by threshold: " + f1Score.collect());

	    JavaRDD<Tuple2<Object, Object>> f2Score = metrics.fMeasureByThreshold(2.0).toJavaRDD();
	    System.out.println("F2 Score by threshold: " + f2Score.collect());

	    // Precision-recall curve
	    JavaRDD<Tuple2<Object, Object>> prc = metrics.pr().toJavaRDD();
	    System.out.println("Precision-recall curve: " + prc.collect());

	  */
	    // Thresholds
	    JavaRDD<Double> thresholds = precision.map(
	      new Function<Tuple2<Object, Object>, Double>() {
	        public Double call(Tuple2<Object, Object> t) {
	          return new Double(t._1().toString());
	        }
	      }
	    );

	    // ROC Curve
	   // JavaRDD<Tuple2<Object, Object>> roc = metrics.roc().toJavaRDD();
	    //System.out.println("ROC curve: " + roc.collect());

	    // AUPRC
	    System.out.println("Area under precision-recall curve = " + metrics.areaUnderPR());


	    // AUROC
	    System.out.println("Area under ROC = " + metrics.areaUnderROC());

	    // Save and load model
	    model.save(sc.sc(), args[1]);
	   // LogisticRegressionModel sameModel = LogisticRegressionModel.load(sc.sc(), args[1]);
	  }
}
