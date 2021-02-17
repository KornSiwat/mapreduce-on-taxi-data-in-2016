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

import java.time.LocalDate;
import java.io.IOException;

public class MostPickupDayOfWeekInJan2016 {

    public static class DayMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            if (key.get() == 0) {
                return;
            }

            String[] data = value.toString().split(",");
            String dateString = data[1].substring(0, 10);
            String day = getDayFromDate(dateString);

            if (!isJan2016(data[1])) return;

            word.set(day);
            context.write(word, one);
        }

        private String getDayFromDate(String dateString) {
            String[] dateInfo = dateString.split("-");
            Integer year = Integer.parseInt(dateInfo[0]);
            Integer month = Integer.parseInt(dateInfo[1]);
            Integer day = Integer.parseInt(dateInfo[2]);

            LocalDate localDate = LocalDate.of(year, month, day);
            String dayOfweek = localDate.getDayOfWeek().toString();

            return  dayOfweek;
        }

        private boolean isJan2016(String dateString) {
            String[] dateInfo = dateString.split("-");
            Integer year = Integer.parseInt(dateInfo[0]);
            Integer month = Integer.parseInt(dateInfo[1]);

            return (year == 2016) && (month == 1);
        }
    }

    public static class KeyValueSwapMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static Text keyOut = new Text("key");
        private static Text valueOut = new Text("val");

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] data = value.toString().trim().replaceAll(" +", " ").split("\\s+");

            keyOut.set(data[1]);
            valueOut.set(data[0]);

            context.write(keyOut, valueOut);
        }
    }

    public static class SumIntReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    public static class DoNothingReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
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
        Job job1 = Job.getInstance(conf1, "Most pickup day of week in January 2016");
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        job1.setJarByClass(MostPickupDayOfWeekInJan2016.class);
        job1.setMapperClass(DayMapper.class);

        job1.setCombinerClass(SumIntReducer.class);
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

        job2.setJarByClass(MostPickupDayOfWeekInJan2016.class);
        job2.setMapperClass(KeyValueSwapMapper.class);

        job2.setReducerClass(DoNothingReducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, countOutputPath);
        FileOutputFormat.setOutputPath(job2, sortedCountOutputPath);

        if (hdfs.exists(countOutputPath)) hdfs.delete(sortedCountOutputPath, true);

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}