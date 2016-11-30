import java.util.Arrays;
import java.util.Iterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import static java.util.Locale.filter;

public class Filter

{

    public static void main(String[] args)
    {

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("FilterProtocol"));
        JavaRDD<String> textFile = sc.textFile(args[0]);

        Function2 filterHeader= new Function2<Integer, Iterator<String>, Iterator<String>>()
        {

            public Iterator<String> call(Integer ind, Iterator<String> iterator) throws Exception
            {
                if(ind==0 && iterator.hasNext())
                {
                    iterator.next();
                    return iterator;
                }
                else
                    return iterator;
            }
        };

        JavaRDD<String> inputRdd = textFile.mapPartitionsWithIndex(removeHeader, false);

        System.out.println("Removed Header");
        JavaRDD<String> filterDataSet = inputRdd.filter(new Function<String, Boolean>()
                {
                    public Boolean call(String s)

                    {
                        String[] columns = new String[11];
                        columns = s.split(",");
                        try
                        {
                            if (Integer.parseInt(columns[4]) == 6)
                            {
                                return true;
                            }

                            else
                                return false;
                        }

                    }
                });

        //filtering for TCP Packets alone
        if(column[2]==TCP)

        {
            System.out.println("Only for TCP");
            JavaRDD<String> newDataSet = filterDataSet.map(new Function<String, String>()
            {
                {
                    public String call (String s)
                    {
                        String[] col = new String[3];
                        col = s.split(",");
                        String newvalues = col[1] + "," + col[2] + "," + col[3];
                        return newvalues;
                    }
                }
            }
            //Exporting to csv
            CsvMapper mapper = new CsvMapper();
            mapper.writer(schema).writeValues(writer).writeAll(newDataSet);
            writer.flush();

        }
    }
}







