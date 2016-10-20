import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

/**
 * Created by paul on 09/10/16.
 */
public class NumberOfFirstNameByNumberOfOrigin {
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, IntWritable> {
        public void map (LongWritable key, Text value, OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
            List<String> line = Arrays.asList(value.toString().split(";"));
            List<String> origin = Arrays.asList(line.get(2).split(","));
            output.collect(new IntWritable(origin.size()), new IntWritable(1));
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{

        public void reduce(IntWritable key, Iterator<IntWritable> iterator, OutputCollector<IntWritable, IntWritable> outputCollector, Reporter reporter) throws IOException {
            int sum = 0;
            while (iterator.hasNext()){
                sum += iterator.next().get();
            }
            outputCollector.collect(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception{
        JobConf jobconf = new JobConf(NumberOfFirstNameByNumberOfOrigin.class);
        jobconf.setJobName("FirstNameByOrigin");

        jobconf.setOutputKeyClass(IntWritable.class);
        jobconf.setOutputValueClass(IntWritable.class);
        jobconf.setMapperClass(Map.class);
        jobconf.setCombinerClass(Reduce.class);
        jobconf.setReducerClass(Reduce.class);

        jobconf.setInputFormat(TextInputFormat.class);
        jobconf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(jobconf, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobconf, new Path(args[1]));

        JobClient.runJob(jobconf);
    }
}
