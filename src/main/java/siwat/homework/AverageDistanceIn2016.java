package siwat.homework;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class AverageDistanceIn2016 {

    public static class DistanceMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
        private static FloatWritable valueOut = new FloatWritable();
        private static Text keyOut = new Text("Average Distance");

        @Override
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            if (key.get() == 0) {
                return;
            }

            String[] data = value.toString().split(",");
            float distance = Float.parseFloat(data[4]);
            valueOut.set(distance);

            if (!is2016(data[1])) return;

            context.write(keyOut, valueOut);
        }

        private boolean is2016(String dateString) {
            String[] dateInfo = dateString.split("-");
            Integer year = Integer.parseInt(dateInfo[0]);

            return year == 2016;
        }
    }

    public static class AverageFloatReducer
            extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        private FloatWritable result = new FloatWritable();

        @Override
        public void reduce(Text key, Iterable<FloatWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            float sum = 0;
            int count = 0;

            for (FloatWritable val : values) {
                sum += val.get();
                count++;
            }

            float average = sum/count;

            result.set(average);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2)
            throw new Exception(
                    "Required: " +
                            "args1: inputPath " +
                            "args2: outputPath");

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Distance");
        job.setInputFormatClass(TextInputFormat.class);

        job.setJarByClass(AverageDistanceIn2016.class);
        job.setMapperClass(DistanceMapper.class);

        job.setCombinerClass(AverageFloatReducer.class);
        job.setReducerClass(AverageFloatReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(outputPath)) hdfs.delete(outputPath, true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}