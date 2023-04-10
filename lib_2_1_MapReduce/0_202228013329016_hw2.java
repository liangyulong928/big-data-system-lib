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
        private Text token = new Text();
        private Text time = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
            while (itr.hasMoreTokens()) {
                String[] record = itr.nextToken().split(" ");
                if(record.length == 3){
                    try {
                        Float.parseFloat(record[2]);
                        token.set(record[0]+" "+record[1]);
                        time.set(record[2]);
                        context.write(token,time);
                    } catch (NumberFormatException e) {
                    }
                }
            }
        }
    }

    public static class CountCombiner extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text token, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            float alltime = 0;
            for (Text val : values) {
                count += 1;
                alltime += Float.parseFloat(val.toString());
            }
            result.set(String.valueOf(count)+" "+String.valueOf(alltime));
            context.write(token, result);
        }
    }

    public static class TimeAveReducer extends Reducer<Text, Text, Text, Text> {
        private Text result_value = new Text();

        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
        }

        public void reduce(Text token, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            float alltime = 0;
            for (Text val : values) {
                String[] split = val.toString().split(" ");
                count += Integer.parseInt(split[0]);
                alltime += Float.parseFloat(split[1]);
            }
            result_value.set(String.valueOf(count) + " " + String.valueOf(alltime/count));
            context.write(token, result_value);
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
        job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " ");
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

