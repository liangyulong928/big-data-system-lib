import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Hw2Part1 {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, FloatWritable> {
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String valueList = value.toString();
            String[] lines = valueList.split("\n");
            for(String line : lines){
                String[] split = line.split(" ");
                if (split.length != 3){
                    continue;
                }
                word.set(split[0]+" "+split[1]);
                context.write(word,new FloatWritable(Float.valueOf(split[2])));
            }
        }
    }

    public static class TimeAveCombiner extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        private FloatWritable result = new FloatWritable();
        private Text countToken = new Text();

        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            float alltime = 0;
            for (FloatWritable val : values) {
                count += 1;
                alltime += val.get();
            }
            result.set(alltime / count);
            String keyString = key.toString();
            keyString = keyString + " "+count;
            countToken.set(keyString);
            context.write(countToken, result);
        }
    }

    public static class TimeAveReducer extends Reducer<Text, FloatWritable, Text, Text> {
        private Text result_key = new Text();
        private Text result_value = new Text();
        private byte[] prefix;
        private byte[] suffix;

        protected void setup(Context context) {
            try {
                prefix = new byte[0];
                suffix = Text.encode(" ").array();
            } catch (Exception e) {
                prefix = suffix = new byte[0];
            }
        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result_key.set(prefix);
            result_key.append(key.getBytes(), 0, key.getLength());
            result_key.append(suffix, 0, suffix.length);
            result_value.set(Integer.toString(sum));
            context.write(result_key, result_value);
        }
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: hw2part1 <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "hw2part1");
        job.setJarByClass(Hw2Part1.class);
        job.setMapperClass(Hw2Part1.TokenizerMapper.class);
        job.setCombinerClass(Hw2Part1.TimeAveCombiner.class);
        job.setReducerClass(Hw2Part1.TimeAveReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

