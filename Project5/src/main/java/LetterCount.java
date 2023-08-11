// ======================= PatternFinder.java ==========================================

/**
 * PatternFinder is a Hadoop MapReduce program that prints the words containing "cool".
 * To run the program, specify the input and output paths as command line arguments.
 * The output will be a text file with words containing "cool"
 * @author - Ruta Deshpande
 * Email - rutasurd@andrew.cmu.edu
 * Andrew id - rutasurd
 */
package org.myorg;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class FindPattern extends Configured implements Tool {
    /**
     * This class extends the Hadoop Mapper class and overrides the map method.
     * The map method reads input line by line, tokenizes it, and gives out each letter as the key with the value of 1.
     * The output of this mapper will be intermediate key-value pairs.
     */
    public static class FindPatternMapper extends Mapper<Object, Text, Text, NullWritable> {
        private final static NullWritable nullWritable = NullWritable.get();
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            while (tokenizer.hasMoreTokens()) {
                String data = tokenizer.nextToken();
                if (data.toLowerCase().contains("cool")) {
                    word.set(data);
                    context.write(word, nullWritable);
                }
            }
        }
    }

    /**
     * This class extends the Hadoop Reducer class and overrides the reduce method.
     * The reduce method receives key-value pairs and sums the values for each key.
     * The output of this reducer will be the final key-value pairs.
     */
    public static class FindPatternReducer extends Reducer<Text, IntWritable, Text, NullWritable>
    {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            context.write(key, NullWritable.get());
        }
    }
    /**
     * This method configures the job, sets input and output paths, mapper and reducer classes, and input and output formats.
     * The job is then submitted to the Hadoop cluster, and the method returns the exit code for the job.
     */
    public int run(String[] args) throws Exception  {

        Job job = new Job(getConf());
        job.setJarByClass(FindPattern.class);
        job.setJobName("findpattern");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setMapperClass(FindPatternMapper.class);
        job.setReducerClass(FindPatternReducer.class);


        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        return success ? 0: 1;
    }

    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        int result = ToolRunner.run(new FindPattern(), args);
        System.exit(result);
    }

}
