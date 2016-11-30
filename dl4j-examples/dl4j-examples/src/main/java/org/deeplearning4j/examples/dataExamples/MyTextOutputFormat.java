package org.deeplearning4j.examples.dataExamples;

import java.io.IOException;
import java.util.*;
import  org.apache.hadoop.fs.FileSystem;
import  org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;

/**
 * Created by avakil on 11/12/16.

public class MyTextOutputFormat extends FileOutputFormat<Text, List<IntWritable>> {

    public MyCustomRecordWriter getRecordWriter(TaskAttemptContext arg0) throws IOException, InterruptedException {
        //get the current path
        MultiLayerConfiguration conf= new MultiLayerConfiguration();
        Path path = FileOutputFormat.getOutputPath(conf);
        //create the full path with the output directory plus our filename
        Path fullPath = new Path(path, "result.txt");

        //create the file in the file system
        FileSystem fs = path.getFileSystem(arg0.getConfiguration());
        FSDataOutputStream fileOut = fs.create(fullPath, arg0);

        //create our record writer with the new file
        return new MyCustomRecordWriter(fileOut);
    }

    @Override
    public org.apache.hadoop.mapred.RecordWriter<Text, List<IntWritable>> getRecordWriter(FileSystem fileSystem, JobConf jobConf, String s, Progressable progressable) throws IOException {
        return null;
    }
}
*/
