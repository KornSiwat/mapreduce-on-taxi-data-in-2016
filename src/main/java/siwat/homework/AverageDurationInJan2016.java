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

public class AverageDurationInJan2016 {

    public static class DurationMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
        private static FloatWritable valueOut = new FloatWritable();
        private static Text keyOut = new Text("Average Duration");

        @Override
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            if (key.get() == 0) {
                return;
            }

            String[] data = value.toString().split(",");
            float duration = computeDuration(data[1], data[2]);
            valueOut.set(duration);

            if (!isJan2016(data[1])) return;

            context.write(keyOut, valueOut);
        }

        private boolean isJan2016(String dateString) {
            String[] dateInfo = dateString.split("-");
            Integer year = Integer.parseInt(dateInfo[0]);
            Integer month = Integer.parseInt(dateInfo[1]);

            return (year == 2016) && (month == 1);
        }

        private float computeDuration(String datetime1, String datetime2) {
            return getTimeFromDateTimeString(datetime2) - getTimeFromDateTimeString(datetime1);
        }

        private float getTimeFromDateTimeString(String dateTime) {
            String[] timeInfo = dateTime.split("\\s+")[1].split(":");
            Integer hour = Integer.parseInt(timeInfo[0]);
            Integer min = Integer.parseInt(timeInfo[1]);
            Integer sec = Integer.parseInt(timeInfo[2]);

            return (hour * 60 * 60) + (min * 60) + sec;
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

        job.setJarByClass(AverageDurationInJan2016.class);
        job.setMapperClass(DurationMapper.class);

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