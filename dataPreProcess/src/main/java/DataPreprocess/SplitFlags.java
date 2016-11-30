package DataPreprocess;

import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class SplitFlags {
	public static void main(String[] args) {

		if (args.length < 2) {
			System.out
					.println("Proper Usage is: /bin/spark-submit --class DataPreprocess.SplitFlags "
							+ "--master local[2] <>/target/DataPreprocess-0.0.1-SNAPSHOT.jar <Path to Input Data>/*.csv <Path to output data>/Output");
			System.exit(0);
		}
		JavaSparkContext sc = new JavaSparkContext(
				new SparkConf().setAppName("SplitFlagsforSVM"));
		JavaRDD<String> textFile = sc.textFile(args[0]);
		// type,SIP,DIP,sPort,dPort,StartTime,EndTime,packets,bytes,flags,duration
		// type,packets,bytes,duration,flags -- New

		int index = 0;
		JavaRDD<String> newDataSet = textFile
				.map(new Function<String, String>() {
					public String call(String s) {
						String[] columns = new String[11];
						columns = s.split(",");
						// column 7
						int length = columns[7].length();

						StringBuilder temp = new StringBuilder();
						for (int i = 0; i < length; i++) {
							temp.append(columns[7].charAt(i) + "|");
						}
						try {
							columns[7] = temp.substring(0, length * 2 - 1);
						} catch (Exception e) {
							System.out.println(columns[0] + "," + columns[1]
									+ "," + columns[2] + "," + columns[3] + ","
									+ columns[4] + "," + columns[5] + ","
									+ columns[6] + "," + columns[7] + ","
									+ columns[8] + "," + columns[9] + ","
									+ columns[10]);

						}
						// columns[7]=temp.toString();
						String newLine = columns[0] + "," + columns[1] + ","
								+ columns[2] + "," + columns[3] + ","
								+ columns[4] + "," + columns[5] + ","
								+ columns[6] + "," + columns[7] + ","
								+ columns[8] + "," + columns[9] + ","
								+ columns[10];
						return newLine;
					}
				});
		System.out.println("Removed Flag Column");

		newDataSet.saveAsTextFile(args[1]);

	}
}
