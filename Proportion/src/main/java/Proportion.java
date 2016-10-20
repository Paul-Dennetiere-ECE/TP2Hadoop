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
 * Created by paul on 14/10/16.
 */
public class Proportion {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, FloatWritable> {
        public void map (LongWritable key, Text value, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
            List<String> line = Arrays.asList(value.toString().split(";"));
            List<String> gender = Arrays.asList(line.get(1).split(","));
            for (Iterator<String> it = gender.iterator(); it.hasNext();){
                String tmpGender = it.next();
                if (tmpGender.equals("m")) {
                    output.collect(new Text("m"), new FloatWritable(1));
                    output.collect(new Text("f"), new FloatWritable(0));
                }
                else {
                    output.collect(new Text("m"), new FloatWritable(0));
                    output.collect(new Text("f"), new FloatWritable(1));
                }
            }

        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, FloatWritable, Text, FloatWritable>{

        public void reduce(Text text, Iterator<FloatWritable> iterator, OutputCollector<Text, FloatWritable> outputCollector, Reporter reporter) throws IOException {
            float sum = 0;
            float size = 0;
            while (iterator.hasNext()){
                sum += iterator.next().get();
                size++;
            }
            float result = (sum/size)*10;
            outputCollector.collect(text, new FloatWritable(result));
        }
    }

    public static void main(String[] args) throws Exception{
        JobConf jobconf = new JobConf(Proportion.class);
        jobconf.setJobName("Proportion");

        jobconf.setOutputKeyClass(Text.class);
        jobconf.setOutputValueClass(FloatWritable.class);
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
