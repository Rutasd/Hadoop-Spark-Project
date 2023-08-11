/**
 * The MaxTemperatureMapper class is a mapper for a Hadoop MapReduce
 * job that extracts the year and temperature from a weather data file
 * and produces key-value pairs for the reducer.
 * The output key is the year and the output value is the temperature.
 * The mapper is used with the MaxTemperatureReducer to find the maximum
 * temperature for each year.
 * @author Ruta Deshpande
 * Email id - rutasurd@andrew.cmu.edu
 * Andrew id - rutasurd
 */
// ============== MaxTemperatureMapper.java ================================
package edu.cmu.andrew.student032;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MaxTemperatureMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    private static final int MISSING = 9999;
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws 			IOException {

        // Get line from input file. This was passed in by Hadoop as value.
        // We have no use for the key (file offset) so we are ignoring it.

        String line = value.toString();

        // Get year when weather data was collected. The year is in positions 15-18.
        // This field is at a fixed position within a line.
        String year = line.substring(15, 19);

        // Get the temperature too.

        int airTemperature;
        if (line.charAt(87) == '+') { // parseInt doesn't like leading plus signs
            airTemperature = Integer.parseInt(line.substring(88, 92));
        } else {
            airTemperature = Integer.parseInt(line.substring(87, 92));
        }

        // Get quality of reading. If not missing and of good quality then
        // produce intermediate (year,temp).

        String quality = line.substring(92, 93);
        if (airTemperature != MISSING && quality.matches("[01459]")) {

            // for each year in input, reduce will be called with
            // (year,[temp,temp,temp, ...])
            // They key is year and the list of temps will be placed in an iterator.

            output.collect(new Text(year), new IntWritable(airTemperature)); }
    }
}