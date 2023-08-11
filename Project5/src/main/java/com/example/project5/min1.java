/**
 * The MinTemperatureMapper class is a mapper for a Hadoop MapReduce
 * job that extracts the year and temperature from a weather data file
 * and produces key-value pairs for the reducer.
 * The output key is the year and the output value is the temperature.
 * The mapper is used with the MinTemperatureReducer to find the minimum
 * temperature for each year.
 * @author Ruta Deshpande
 * Email id - rutasurd@andrew.cmu.edu
 * Andrew id - rutasurd
 */
// ============== MinTemperatureMapper.java ================================
package edu.cmu.andrew.student032;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MinTemperatureMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    private static final int MISSING = 9999;

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        String line = value.toString();

        String year = line.substring(15, 19);

        int airTemperature;
        if (line.charAt(87) == '+') {
            airTemperature = Integer.parseInt(line.substring(88, 92));
        } else {
            airTemperature = Integer.parseInt(line.substring(87, 92));
        }
        airTemperature /= 10; // Convert temperature to Celsius

        String quality = line.substring(92, 93);
        if (airTemperature != MISSING && quality.matches("[01459]")) {
            output.collect(new Text(year), new IntWritable(airTemperature));
        }
    }
}