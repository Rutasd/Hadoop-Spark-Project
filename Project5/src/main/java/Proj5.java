// ======================= WordCount.java ==========================================

/**
 * WordCount is a Hadoop MapReduce program that counts the frequency of each word in a text input file.
 * To run the program, specify the input and output paths as command line arguments.
 * The output will be a text file with each line containing a word
 * and the number of times it appeared in the input data.
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class WordCount extends Configured implements Tool {
    /**
     * This class extends the Hadoop Mapper class and overrides the map method.
     * The map method reads input line by line, tokenizes it, and gives out each word as the key with the value of 1.
     * The output of this mapper will be intermediate key-value pairs.
     */
    public static class WordCountMap extends Mapper<LongWritable, Text, Text, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while(tokenizer.hasMoreTokens())
            {
                word.set(tokenizer.nextToken());
                context.write(word, one);
            }
        }
    }

    /**
     * This class extends the Hadoop Reducer class and overrides the reduce method.
     * The reduce method receives key-value pairs and sums the values for each key.
     * The output of this reducer will be the final key-value pairs.
     */
    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int sum = 0;
            for(IntWritable value: values)
            {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }

    }
    /**
     * This method configures the job, sets input and output paths, mapper and reducer classes, and input and output formats.
     * The job is then submitted to the Hadoop cluster, and the method returns the exit code for the job.
     */
    public int run(String[] args) throws Exception  {

        Job job = new Job(getConf());
        job.setJarByClass(WordCount.class);
        job.setJobName("wordcount");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(WordCountMap.class);
        job.setReducerClass(WordCountReducer.class);


        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        return success ? 0: 1;
    }


    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        int result = ToolRunner.run(new WordCount(), args);
        System.exit(result);
    }

}
