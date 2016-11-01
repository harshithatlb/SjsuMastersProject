package edu.sjsu.ddos.dataPreProces;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.StandardScalerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CrossValidationLR {

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
	    
	    DataFrame[] splits = assembled.randomSplit(new double[] {0.7, 0.3});
	    DataFrame trainingData = splits[0];
	    DataFrame testData = splits[1];
	    
	    StandardScaler scaler = new StandardScaler()
	    .setInputCol("features")//.setInputCol("bytes").setInputCol("duration")
	    .setOutputCol("scaledfeatures")//.setOutputCol("scaledBytes").setOutputCol("scaledDuration")
	    .setWithStd(true)
	    .setWithMean(false);
	   

	  // Compute summary statistics by fitting the StandardScaler
	  /*StandardScalerModel scalerModel = scaler.fit(assembled);

	  // Normalize each feature to have unit standard deviation.
	  DataFrame scaledData = scalerModel.transform(assembled);
	  
	  scaledData.show();*/
	  
	  LogisticRegression lr = new LogisticRegression().setLabelCol("label").setFeaturesCol("scaledfeatures")
	  .setMaxIter(10)
	  .setRegParam(0.01);
	Pipeline pipeline = new Pipeline()
	  .setStages(new PipelineStage[] {scaler, lr});

	// We use a ParamGridBuilder to construct a grid of parameters to search over.
	// With 3 values for hashingTF.numFeatures and 2 values for lr.regParam,
	// this grid will have 3 x 2 = 6 parameter settings for CrossValidator to choose from.
	ParamMap[] paramGrid = new ParamGridBuilder()
	    .addGrid(lr.regParam(), new double[]{0.2,0.1,0.01,0.5,0.3})
	    .addGrid(lr.fitIntercept())
	    .addGrid(lr.elasticNetParam(), new double[] {0.5, 0.8,1.0})
	    .build();

	// We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
	// This will allow us to jointly choose parameters for all Pipeline stages.
	// A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
	// Note that the evaluator here is a BinaryClassificationEvaluator and its default metric
	// is areaUnderROC.
	CrossValidator cv = new CrossValidator()
	  .setEstimator(pipeline)
	  .setEvaluator(new BinaryClassificationEvaluator())
	  .setEstimatorParamMaps(paramGrid)
	  .setNumFolds(3); // Use 3+ in practice

	// Run cross-validation, and choose the best set of parameters.
	CrossValidatorModel cvModel = cv.fit(trainingData);
	
	
	//System.out.println("elasticNetParam"+cvModel.getParam("elasticNetParam"));

	// Make predictions on test documents. cvModel uses the best model found (lrModel).
	/*DataFrame predictions = cvModel.transform(testData);
	for (Row r: predictions.select("label", "bytes", "probability", "prediction").collect()) {
	  System.out.println("(" + r.get(0) + ", " + r.get(1) + ") --> prob=" + r.get(2)
	      + ", prediction=" + r.get(3));
	}*/
	}
	
	public static <T> List<List<T>> zip(List<T>... lists) {
	    List<List<T>> zipped = new ArrayList<List<T>>();
	    for (List<T> list : lists) {
	        for (int i = 0, listSize = list.size(); i < listSize; i++) {
	            List<T> list2;
	            if (i >= zipped.size())
	                zipped.add(list2 = new ArrayList<T>());
	            else
	                list2 = zipped.get(i);
	            list2.add(list.get(i));
	        }
	    }
	    return zipped;
	}
}
