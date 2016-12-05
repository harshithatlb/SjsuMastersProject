package logisticRegression;

import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;

import scala.Tuple2;


public class LoadLR4Predictoion {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("Get NetFlow");
		JavaStreamingContext jssc = new JavaStreamingContext(conf,
				Durations.seconds(5));
		// Create a DStream that will connect to hostname:port, like
		// localhost:9999

		JavaReceiverInputDStream<String> lines = jssc.socketTextStream(args[1],
				Integer.parseInt(args[2]));
		final LogisticRegressionModel sameModel = LogisticRegressionModel.load(
				args[0]);
		System.out.println(lines);
		JavaDStream<Tuple2<String, Vector>> newDataSet = lines
				.map(new Function<String, Tuple2<String, Vector>>() {
					public Tuple2<String, Vector> call(String key) {
						double[] feature = new double[8];
						int i = 0;
						for (String s : key.split(",")) {
							feature[i] = Double.parseDouble(s);
							i++;
						}
						return new Tuple2<String, Vector>(key, Vectors
								.dense(feature));

					}
				});
		
		JavaDStream<Tuple2<String, Row>> rowDataSet = newDataSet.map(new Function<Tuple2<String, Vector>,Tuple2<String, Row>>(){
			public Tuple2<String, Row> call(Tuple2<String, Vector> z){
				return new Tuple2<String, Row>(z._1(),RowFactory.create(z._2()));
			}		
		});
		
		JavaDStream<Tuple2<String, Row>> rowDfDataSet = newDataSet.map(new Function<Tuple2<String, Vector>,Tuple2<String, Row>>(){
			public Tuple2<String, Row> call(Tuple2<String, Vector> z){
				return new Tuple2<String, Row>(z._1(),RowFactory.create(z._2()));
			}		
		});

		
		JavaDStream<Tuple2<String, Double>> result = newDataSet
				.map(new Function<Tuple2<String, Vector>, Tuple2<String, Double>>() {
					public Tuple2<String, Double> call(Tuple2<String, Vector> z) {
						Tuple2<String, Double> preResults = new Tuple2<String, Double>(
								z._1(), sameModel.predict(z._2()));
						return preResults;
					}
				});

		result.print();

		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
