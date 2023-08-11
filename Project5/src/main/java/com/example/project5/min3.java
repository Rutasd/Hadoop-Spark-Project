/**
 * The MinTemperature class is the main driver class for a Hadoop MapReduce
 * job that finds the min temperature for each year in the input file.
 * It configures and runs the MapReduce job using the MinTemperatureMapper and
 * MinTemperatureReducer classes.
 * @author - Ruta Deshpande
 * email - rutasurd@andrew.cmu.edu
 * Andrew id - rutasurd
 */
// ============== MinTemperature.java ================================
package edu.cmu.andrew.student032;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class MinTemperature {
    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Usage: MinTemperature <input path> <output path>");
            System.exit(-1);
        }
        JobConf conf = new JobConf(MinTemperature.class);
        conf.setJobName("Min temperature");

        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        conf.setMapperClass(MinTemperatureMapper.class);
        conf.setReducerClass(MinTemperatureReducer.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        JobClient.runJob(conf);
    }
}