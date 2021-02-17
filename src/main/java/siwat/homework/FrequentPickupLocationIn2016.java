package siwat.homework;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class FrequentPickupLocationIn2016 {

    public static class LocationMapper extends Mapper<LongWritable,Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            if (key.get() == 0) {
                return;
            }

            String[] data = value.toString().split(",");
            float pickUpLongitude = Float.parseFloat(data[5]);
            float pickUpLatitude = Float.parseFloat(data[6]);
            String location = String.format("(%.4f,%.4f)", pickUpLatitude, pickUpLongitude);

            boolean isDataNotValid = (pickUpLatitude == 0) && (pickUpLongitude == 0);
            if (isDataNotValid) return;

            if (!is2016(data[1])) return;

            word.set(location);
            context.write(word, one);
        }

        private boolean is2016(String dateString) {
            String[] dateInfo = dateString.split("-");
            Integer year = Integer.parseInt(dateInfo[0]);

            return (year == 2016);
        }
    }

    public static class KeyValueSwapMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private static IntWritable keyOut = new IntWritable();
        private static Text valueOut = new Text("val");

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] data = value.toString().trim().replaceAll(" +", " ").split("\\s+");

            keyOut.set(Integer.parseInt(data[1]));
            valueOut.set(data[0]);

            context.write(keyOut, valueOut);
        }
    }

    public static class SumIntReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    public static class DoNothingReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        private Text result = new Text();

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value: values) {
                result.set(value);
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) throw new Exception(
                "Required: " +
                        "args1: inputPath " +
                        "args2: countOutputPath " +
                        "args3: sortedCountOutputPath");

        Path inputPath = new Path(args[0]);
        Path countOutputPath = new Path(args[1]);
        Path sortedCountOutputPath = new Path(args[2]);

        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Count Pickup Location in 2016");
        job1.setInputFormatClass(TextInputFormat.class);

        job1.setJarByClass(FrequentPickupLocationIn2016.class);
        job1.setMapperClass(LocationMapper.class);

        job1.setReducerClass(SumIntReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, countOutputPath);

        FileSystem hdfs = FileSystem.get(conf1);
        if (hdfs.exists(countOutputPath)) hdfs.delete(countOutputPath, true);

        job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Sort Count");
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        job2.setJarByClass(FrequentPickupLocationIn2016.class);
        job2.setMapperClass(KeyValueSwapMapper.class);

        job2.setReducerClass(DoNothingReducer.class);

        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, countOutputPath);
        FileOutputFormat.setOutputPath(job2, sortedCountOutputPath);

        if (hdfs.exists(countOutputPath)) hdfs.delete(sortedCountOutputPath, true);

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}