import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.Dataflow;

public class Randomforest

{
    public static void main(String[] args)

    {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("RandomForest"));
        JavaRDD<String> textFile = sc.textFile(args[0]);
        JavaRDD<Tuple2<String, LabeledPoint>> newDataSet = textFile.map(new Function<String, Tuple2<String, LabeledPoint>>()

        {
            public Tuple2<String, LabeledPoint> call(String s)

            {
                String[] parts = new String[3];
                parts = s.split(",");
                double[] points = new double[parts.length - 1];
                for (int i = 1; i < (parts.length); i++) {
                    points[i - 1] = Double.valueOf(parts[i]);
                }
                return new Tuple2<String, LabeledPoint>(parts[0],
                        new LabeledPoint(Double.valueOf(parts[0]), Vectors.dense(points)));

            }
        });

        JavaRDD<LabeledPoint> labeledSet = mapper.map(new Function<Tuple2<String, LabeledPoint>, LabeledPoint>() {
            public LabeledPoint call(Tuple2<String, LabeledPoint> z)

            {
                return z._2();

            }
        });
        //splitup of the model for 70% and 30 %
        JavaRDD<LabeledPoint>[] labeledset = data.randomSplit(new double[]{0.7, 0.3});
        JavaRDD<LabeledPoint> trainingData = splits[0];
        JavaRDD<LabeledPoint> testData = splits[1];

        //Trainmodel

        String treeStrategy = Strategy.defaultStrategy("Classification");
        HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
        Integer numTrees = 3;
        String featureSubsetStrategy = "auto";
        Integer maxDepth = 5;
        Integer maxBins = 32;
        Integer seed = 12345;

        final RandomForestModel model = RandomForest.trainClassifier(trainingData, categoricalFeaturesInfo, numTrees, featureSubsetStrategy, maxDepth, maxBins, seed);

        Double testErr = 1.0 * predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>()
        {
            public Boolean call(Tuple2<Double, Double> pl)
            {
                return !pl._1().equals(pl._2());
            }
        }).count() / testData.count();

        System.out.println("Test Error: " + testErr);

        System.out.println("Learned classification forest model:\n" + model.toDebugString());

    }
}




