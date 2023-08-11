/**
 * The MinTemperatureReducer class finds the minimum temperature
 * for each year in the input file.
 * The output key is the year and the output value is the minimum temperature.
 * @author - Ruta Deshpande
 * Email id - rutasurd@andrew.cmu.edu
 * Andrew id - rutasurd
 */
// ============== MinTemperatureReducer.java ================================
package edu.cmu.andrew.student032;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class MinTemperatureReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        int minValue = Integer.MAX_VALUE;
        while (values.hasNext()) {
            int value = values.next().get();
            if (value < minValue) {
                minValue = value;
            }
        }
        output.collect(key, new IntWritable(minValue));
    }
}