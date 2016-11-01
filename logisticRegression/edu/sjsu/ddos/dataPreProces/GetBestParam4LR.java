package edu.sjsu.ddos.dataPreProces;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.classification.BinaryLogisticRegressionSummary;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.classification.LogisticRegressionTrainingSummary;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.StandardScalerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;


public class GetBestParam4LR {
	public static void main(String[] args) {
		JavaSparkContext sc = new JavaSparkContext(
				new SparkConf().setAppName("LogisticRegression"));
	    JavaRDD<String> textFile=sc.textFile(args[0]);

	    // $example on$
	    // Load training data
	    //Label,sIP,dIP,sPort,dPort,sTime,eTime,flags,packets,bytes,duration,A, F, P, R, S
		  //	0,  1,  2,    3,   4,     5,     6,   7,    8,       9,  10	  ,11 12 13 14 15
		  //Total 11
		  // For Prediction -- 7,8,9,10
	    
	    
	    
	    JavaRDD<String> dummyFile = textFile.map(new Function<String,String>() {
			public String call(String s) {
				String[] parts = new String[11];
				parts = s.split(",");
				String flag= parts[7];
				 int[] flagsBool = new int[5];
				//A F P R S
				//0 1 2 3 4 
				if (flag.contains("A"))
					flagsBool[0]=1;
				
				if (flag.contains("F"))
					flagsBool[1]=1;
			    if (flag.contains("P"))
					flagsBool[2]=1;
			    
			    if (flag.contains("R"))
					flagsBool[3]=1;
			    
			    if (flag.contains("S"))
					flagsBool[4]=1;
					
		    	     //String key = parts[0]+","+parts[1]+","+parts[2]+","+parts[3]+","+parts[4]+","+parts[5]+","+parts[6]+","+parts[8]+","+parts[9]+","+parts[10];
		    	 String partsString = String.join(",", parts);
		    	 String finalString = partsString+","+flagsBool[0]+","+flagsBool[1]+","+flagsBool[2]+","+flagsBool[3]+","+flagsBool[4];
		    	// System.
				return finalString;
}
		});
	    
	    SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

	    // The schema is encoded in a string
	    String schemaString = "Label,sIP,dIP,sPort,dPort,sTime,eTime,flags,packets,bytes,duration,A,F,P,R,S";

	    // Generate the schema based on the string of schema
	    List<StructField> fields = new ArrayList<StructField>();
	    
	    /*for (String fieldName: schemaString.split(",")) {
	      fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
	    }*/
	    
	    fields.add(DataTypes.createStructField("label", DataTypes.DoubleType, true));
	    fields.add(DataTypes.createStructField("sIP", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("dIP", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("sPort", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("dPort", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("sTime", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("eTime", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("flags", DataTypes.StringType, true));
	    fields.add(DataTypes.createStructField("packets", DataTypes.DoubleType, true));
	    fields.add(DataTypes.createStructField("bytes", DataTypes.DoubleType, true));
	    fields.add(DataTypes.createStructField("duration", DataTypes.DoubleType, true));
	    fields.add(DataTypes.createStructField("A", DataTypes.DoubleType, true));
	    fields.add(DataTypes.createStructField("F", DataTypes.DoubleType, true));
	    fields.add(DataTypes.createStructField("P", DataTypes.DoubleType, true));
	    fields.add(DataTypes.createStructField("R", DataTypes.DoubleType, true));
	    fields.add(DataTypes.createStructField("S", DataTypes.DoubleType, true));
	    StructType schema = DataTypes.createStructType(fields);

	    // Convert records of the RDD (people) to Rows.
	    JavaRDD<Row> rowRDD = dummyFile.map(
	      new Function<String, Row>() {
	        public Row call(String record) throws Exception {
	          String[] fields = record.split(",");
	          return RowFactory.create(Double.parseDouble(fields[0]) , fields[1].trim(),fields[2],fields[3],fields[4],fields[5],fields[6],fields[7],Double.parseDouble(fields[8]),Double.parseDouble(fields[9]),Double.parseDouble(fields[10]),Double.parseDouble(fields[11]),Double.parseDouble(fields[12]),Double.parseDouble(fields[13]),Double.parseDouble(fields[14]),Double.parseDouble(fields[15]));
	        }
	      });

	    // Apply the schema to the RDD.
	    DataFrame flowDataFrame = sqlContext.createDataFrame(rowRDD, schema);
	    
	    flowDataFrame.show();
	    
	    VectorAssembler assembler = new VectorAssembler().setInputCols(new String[]{"packets","bytes","duration","A","F","P","R","S"}).setOutputCol("features");
	    
		 
	    DataFrame assembled = assembler.transform(flowDataFrame);
	    
	    StandardScaler scaler = new StandardScaler()
	    .setInputCol("features")//.setInputCol("bytes").setInputCol("duration")
	    .setOutputCol("scaledfeatures")//.setOutputCol("scaledBytes").setOutputCol("scaledDuration")
	    .setWithStd(true)
	    .setWithMean(false);
	   

	  // Compute summary statistics by fitting the StandardScaler
	  StandardScalerModel scalerModel = scaler.fit(assembled);

	  // Normalize each feature to have unit standard deviation.
	  DataFrame scaledData = scalerModel.transform(assembled);
	  
	  scaledData.show();
    

	    LogisticRegression lr = new LogisticRegression().setStandardization(true)
	      .setMaxIter(10)
	      .setRegParam(0.3)
	      .setElasticNetParam(0.0);

	    // Fit the model
	    LogisticRegressionModel lrModel = lr.fit(assembled);

	    // Print the coefficients and intercept for logistic regression
	    System.out.println("Coefficients: "
	      + lrModel.coefficients() + " Intercept: " + lrModel.intercept()+ "getRegParam "+lrModel.getRegParam()+" getElasticNetParam "+lrModel.getElasticNetParam()+" getThreshold "+lrModel.getThreshold());
	    // $example off$

	 // Extract the summary from the returned LogisticRegressionModel instance trained in the earlier
	    // example
	    LogisticRegressionTrainingSummary trainingSummary = lrModel.summary();

	    // Obtain the loss per iteration.
	    double[] objectiveHistory = trainingSummary.objectiveHistory();
	    for (double lossPerIteration : objectiveHistory) {
	      System.out.println(lossPerIteration);
	    }

	    // Obtain the metrics useful to judge performance on test data.
	    // We cast the summary to a BinaryLogisticRegressionSummary since the problem is a binary
	    // classification problem.
	    BinaryLogisticRegressionSummary binarySummary =
	      (BinaryLogisticRegressionSummary) trainingSummary;

	    // Obtain the receiver-operating characteristic as a dataframe and areaUnderROC.
	    DataFrame roc = binarySummary.roc();
	    roc.show();
	    roc.select("FPR").show();
	    System.out.println(binarySummary.areaUnderROC());

	    // Get the threshold corresponding to the maximum F-Measure and rerun LogisticRegression with
	    // this selected threshold.
	    DataFrame fMeasure = binarySummary.fMeasureByThreshold();
	    double maxFMeasure = fMeasure.select(functions.max("F-Measure")).head().getDouble(0);
	    double bestThreshold = fMeasure.where(fMeasure.col("F-Measure").equalTo(maxFMeasure))
	      .select("threshold").head().getDouble(0);
	    lrModel.setThreshold(bestThreshold);
	    // $example off$

	    sc.stop();
	  }
}
