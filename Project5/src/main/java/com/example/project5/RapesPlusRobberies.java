/**
 * This MapReduce program takes in a text file containing crime data and
 * outputs the total number of rapes and robberies
 * that occurred, separated by crime type.
 * This program defines two classes - RapesPlusRobberiesMapper and RapesPlusRobberiesReducer - which are responsible for
 * mapping and reducing the data, respectively.
 * @author Ruta Deshpande
 * Email - rutasurd@andrew.cmu.edu
 * Andrew id - rutasurd
 */

package edu.cmu.andrew.student032;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import java.io.IOException;

public class RapesPlusRobberies extends Configured implements Tool {

    public static class RapesPlusRobberiesMapper extends Mapper<LongWritable, Text, NullWritableZ, IntWritable> {
        private final static IntWritable ONE = new IntWritable(1);
        private Text text = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String textLine = value.toString();
            if (!textLine.startsWith("X")) {
                String[] data = textLine.split("\t");
                if (data.length >= 5) {
                    String rapeOrRobbery = data[4].toLowerCase();
                    if (rapeOrRobbery.equalsIgnoreCase("rape") || rapeOrRobbery.equalsIgnoreCase("robbery")) {
                        text.set(rapeOrRobbery);
                        context.write(text, ONE);
                    }
                }
            }
        }
    }

    public static class RapesPlusRobberiesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "RapesAndRobberies");
        job.setJarByClass(getClass());

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(RapesPlusRobberiesMapper.class);
        job.setCombinerClass(RapesPlusRobberiesReducer.class);
        job.setReducerClass(RapesPlusRobberiesReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new RapesPlusRobberies(), args);
        System.exit(exitCode);
    }
}
