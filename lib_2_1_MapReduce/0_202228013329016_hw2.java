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


/**
 *  This code is used to record communication times and average call time in Hadoop programming experiment
 *
 *  @author     Liang Yulong
 *  @version    1.0
 *  @since      2023-04-09
 */
public class Hw2Part1 {
    /**
     * The TokenizerMapper class is used to extract the two sides of the call and obtain the call time in each call record.
     *
     * reference: http://hadoop.apache.org/docs/r2.6.0/api/org/apache/hadoop/mapreduce/Mapper.html
     */
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        private Text token = new Text();
        private Text time = new Text();

        /**
         *
         * Perform map operations after extracting records
         *
         * @param key       The key of the input data is not processed here
         * @param value     Call record set
         * @param context   System and user profile
         * @throws IOException
         * @throws InterruptedException
         * @throws NumberFormatException    To solve the problem that the call duration is dirty data
         */
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

    /**
     *  CountCombiner is used to preprocess the map result for subsequent reduce operations
     */
    public static class CountCombiner extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        /**
         * Collect the number of calls and total time between two parties on the local device
         *
         * @param token     Both sides of the conversation
         * @param values    token Duration of each call
         * @param context   System and user profile
         * @throws IOException
         * @throws InterruptedException
         */
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

    /**
     * TimeAveReducer Indicates the total number of calls and average call time
     */
    public static class TimeAveReducer extends Reducer<Text, Text, Text, Text> {
        private Text result_value = new Text();

        /**
         * Configure Reduce operations
         *
         * @param context   System and user profile
         */
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
        }

        /**
         *
         * Calculates the number of calls and average call time between two communication parties
         * @param token     Both sides of the conversation
         * @param values    token call times and total call time
         * @param context   System and user profile
         * @throws IOException
         * @throws InterruptedException
         */
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

    /**
     * Configure MapReduce projects
     *
     * @param args  Consists of more than one input document path and one output document path
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     * @throws NumberFormatException
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, NumberFormatException{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("There is at least one input file path and one output file location.");
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

