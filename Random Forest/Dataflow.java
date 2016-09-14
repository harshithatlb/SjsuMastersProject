import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.Randomforest;

public class Dataflow
{

    public static void main(String[] args)
    {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("Random Forest");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream(args[1], Integer.parseInt(args[2]));
        System.out.println(lines);
        JavaDStream<Tuple2<String, LabeledPoint>> newDataSet = lines.map(new Function<String,Tuple2<String, LabeledPoint>>() {
            public Tuple2<String, LabeledPoint> call(String s)
            {
                String[] parts = new String[11];
                parts = s.split(",");
                double[] points = new double[3];
                points[0] = Double.valueOf(parts[8]);
                points[1] = Double.valueOf(parts[9]);
                points[2] = Double.valueOf(parts[10]);
                String key = parts[0]+","+parts[1]+","+parts[2]+","+parts[3]+","+parts[4]+","+parts[5]+","+parts[6]+","+parts[7];
                return new Tuple2<String, LabeledPoint>(key, new LabeledPoint(Double.valueOf(parts[0]), Vectors.dense(points)));

            }
        });


        JavaDstream<Double, Double> predictionAndLabel = testData.mapToPair(new PairFunction<LabeledPoint, Double, Double>()
        {
            public Tuple2<Double, Double> call(LabeledPoint p)
            {
                return new Tuple2<>(model.predict(p.features()), p.label());
            }
        });

        final RandomForestModel model = RandomForest.trainClassifier(jssc.sc().sc(), args[0]);

        model.save(jsc.sc(), "/usr/Home/Sudarshan/295B");
        RandomForestModel sameModel = RandomForestModel.load(jsc.sc(), "/usr/Home/Sudarshan/295B");


    }
}