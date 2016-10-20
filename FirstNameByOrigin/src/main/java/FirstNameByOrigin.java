/**
 * Created by paul on 08/10/16.
 */
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
public class FirstNameByOrigin {
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        public void map (LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            List<String> line = Arrays.asList(value.toString().split(";"));
            List<String> origin = Arrays.asList(line.get(2).split(", "));
            for (Iterator<String> it = origin.iterator(); it.hasNext();){
                output.collect(new Text(it.next()), new IntWritable(1));
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{

        public void reduce(Text text, Iterator<IntWritable> iterator, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            int sum = 0;
            while (iterator.hasNext()){
                sum += iterator.next().get();
            }
            outputCollector.collect(text, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception{
        JobConf jobconf = new JobConf(FirstNameByOrigin.class);
        jobconf.setJobName("FirstNameByOrigin");

        jobconf.setOutputKeyClass(Text.class);
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
