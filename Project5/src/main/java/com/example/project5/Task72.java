/**
 * This MapReduce program takes in a text file containing crime data for the city of Pittsburgh, PA and
 * outputs the KML file of coordinates of crimes that occurred within 350 meters of 3803 Forbes Avenue in Oakland.
 * This program defines two classes - AggravatedAssaultsKMLMapper and AggravatedAssaultsKMLReducer - which are responsible for
 * mapping and reducing the data, respectively.
 * The output is the total number of aggravated assault crimes within the given area
 * Author: Ruta Deshpande
 * Email: rutasurd@andrew.cmu.edu
 * Andrew ID: rutasurd
 */

package edu.cmu.andrew.student032;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class AggravatedAssaultsKML extends Configured implements Tool {
    private final static double X = 1354326.897;
    private final static double Y = 411447.7828;
    private final static double MAX_DISTANCE = 350 * 350;

    /**
     * Main Method
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new AggravatedAssaultsKML(), args);
        System.exit(exitCode);
    }

    /**
     * Run method for setting job attributes
     * @param args
     * @return
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        job.setJarByClass(AggravatedAssaultsKML.class);
        job.setJobName("Oakland Crime Stats");

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(AggravatedAssaultsKMLMapper.class);
        job.setReducerClass(AggravatedAssaultsKMLReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Mapper class for sending data of coordinates within 350m of given coordinates
     */
    public static class AggravatedAssaultsKMLMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        private final static double X = 1354326.897;
        private final static double Y = 411447.7828;
        private final static double MAX_DISTANCE = 350 * 350;

        /**
         * map method sends a kml file to the reducer which has the coordinates of
         * the places where crimes have occured
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String textLine = value.toString();
            if (!textLine.startsWith("X")) {
                String[] data = textLine.split("\t");
                if (data.length >= 5) {
                    String crimeType = data[4].toLowerCase();
                    if (crimeType.toLowerCase().contains("aggravated assault")) {
                        double x_coordinate = Double.parseDouble(data[0]);
                        double y_coordinate = Double.parseDouble(data[1]);
                        double dist = Math.sqrt(Math.pow(x_coordinate - X, 2) + Math.pow(y_coordinate - Y, 2));
                        dist = dist * 0.3048;
                        if (dist < 350.0) {
                            String lat = data[7];
                            String lon = data[8];
                            String name = "Aggravated Assault";
                            String description = "Occurred within 350 meters of 3803 Forbes Avenue in Oakland";
                            StringBuilder kmlBuilder = new StringBuilder();
                            kmlBuilder.append("<Placemark>\n");
                            kmlBuilder.append("<name>").append(name).append("</name>\n");
                            kmlBuilder.append("<description>").append(description).append("</description>\n");
                            kmlBuilder.append("<Point>\n");
                            kmlBuilder.append("<coordinates>").append(lon).append(",").append(lat).append(",0</coordinates>\n");
                            kmlBuilder.append("</Point>\n");
                            kmlBuilder.append("</Placemark>");

                            String kmlOutput = kmlBuilder.toString();
                            context.write(NullWritable.get(), new Text(kmlOutput));
                        }
                    }
                }
            }
        }
    }

    /**
     * Reducer class
     */
    public static class AggravatedAssaultsKMLReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
        /**
         * Reducer method to add other xml tags and complete the kml file according to all points received from the mapper
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder kmlContent = new StringBuilder();
            kmlContent.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
            kmlContent.append("<kml xmlns=\"http://www.opengis.net/kml/2.2\">\n");
            kmlContent.append("<Document>\n");

            for (Text value : values) {
                kmlContent.append(value.toString());
                kmlContent.append("\n");
            }

            kmlContent.append("</Document>\n");
            kmlContent.append("</kml>");
            kmlContent.append("</xml");

            context.write(NullWritable.get(), new Text(kmlContent.toString()));
        }
    }
}
