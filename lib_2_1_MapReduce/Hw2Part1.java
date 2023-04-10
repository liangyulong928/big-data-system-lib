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
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        private Text word = new Text();
        private Text one = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException, NumberFormatException {
            String valueList = value.toString();
            StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
            while (itr.hasMoreTokens()) {
                String[] token = itr.nextToken().split(" ");
                word.set(token[0]+" "+token[1]);
                one.set(token[2]);
                context.write(word,one);
            }
        }
    }

    public static class CountCombiner extends Reducer<Text, Text, Text, Text> {
        private Text countToken = new Text();
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            float alltime = 0;
            for (Text val : values) {
                count += 1;
                alltime += Float.parseFloat(val.toString());
            }
            result.set(String.valueOf(count)+" "+String.valueOf(alltime));
            countToken.set(key.toString());
            context.write(countToken, result);
        }
    }

    public static class TimeAveReducer extends Reducer<Text, Text, Text, Text> {
        private Text result_key = new Text();
        private Text result_value = new Text();
        private byte[] suffix;

        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            conf.set("mapreduce.output.textoutputformat.separator", " ");
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            float alltime = 0;
            for (Text val : values) {
                String result = val.toString();
                String[] split = result.split(" ");
                count += Integer.parseInt(split[0]);
                alltime += Float.parseFloat(split[1]);
            }
            result_key.set(key);
            result_value.set(String.valueOf(count) + " " + String.valueOf(alltime/count));
            context.write(result_key, result_value);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, NumberFormatException{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: hw2part1 <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "hw2part1");
        job.setJarByClass(Hw2Part1.class);
        job.setMapperClass(Hw2Part1.TokenizerMapper.class);
        job.setCombinerClass(Hw2Part1.CountCombiner.class);
        job.setReducerClass(Hw2Part1.TimeAveReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

