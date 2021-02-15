package project;

import java.io.IOException;
import java.util.StringTokenizer;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class AvgRevNumber extends Configured implements Tool{

    public static class UserMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        JSONParser parser = new JSONParser();
        String userId;
        int revCount;

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //read json line
            Object obj = null;
            try {
                obj = parser.parse(value.toString());
            } catch (ParseException e) {
                System.out.println("position: " + e.getPosition());
                e.printStackTrace();
            }
            JSONObject jsonObject = (JSONObject) obj;

            //get interesting fields
            userId = (String) jsonObject.get("user_id");
            long tmp = ((long) jsonObject.get("review_count"));
            revCount = (int) tmp;

            //write everything to reducer.
            context.write(new Text(userId), new IntWritable(revCount) );

        }
    }

    public static class ReviewMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        JSONParser parser = new JSONParser();
        String businessId;
        String userId;

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //read json line
            Object obj = null;
            try {
                obj = parser.parse(value.toString());
            } catch (ParseException e) {
                System.out.println("position: " + e.getPosition());
                e.printStackTrace();
            }
            JSONObject jsonObject = (JSONObject) obj;

            //get interesting fields
            businessId = (String) jsonObject.get("business_id");
            userId = (String) jsonObject.get("user_id");

            //write to reducer only if it's the correct business
            if (businessId.equals("d4qwVw4PcN-_2mK2o1Ro1g")){
                context.write(new Text(userId), new IntWritable(-1) );
            }
        }
    }

    public static class GroupMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //read line member: (review count)
            StringTokenizer st = new StringTokenizer(value.toString());
            int reviewCount = Integer.parseInt(st.nextToken());

            context.write(new Text(""), new IntWritable(reviewCount));

        }
    }

    public static class JoinReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        boolean isReviewer;
        boolean userExists;
        int review_count;

        @Override
        public void reduce(Text user_id, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            /*
             * Note on values:
             * From UserMapper comes 1 review count (or nothing, if this user is not in the small_table).
             * From ReviewMapper come 0 to X "-1", One for each review on the business from this user.
             */

            isReviewer = false;
            userExists = false;

            /*
             * iteration on counts:
             * decide if this is a correct line
             */
            for (IntWritable v: values){

                if (v.get() == -1){
                    // this user has written at least one review for the business
                    isReviewer = true;
                }
                else{
                    // this user exists in user_table aka he/she has a review count
                    userExists = true;
                    review_count = v.get();
                }
            }

            /*
             * proceed only if the user is a reviewer of the business
             * and he/she exists in user table
             */

            if (isReviewer && userExists){
                context.write( new Text(""), new IntWritable(review_count) );
            }
        }
    }

    public static class AvgReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Text useless, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {

            /*
             * receives every count (key is always empty).
             * compute avg on all counts.
             */

            int sum = 0, tot = 0;

            for (IntWritable c : counts){
                sum += c.get();
                tot ++;
            }

            context.write(new Text(""), new DoubleWritable((double) sum/tot));
        }
    }


    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 4) {
            System.err.printf("%s requires four arguments:\n" +
                            "user table path, review table path, path tmp files, output path\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        //start job 1
        Configuration conf1 = getConf();
        Job job1 = new Job(conf1, "Average reviews number 1st Job");

        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, UserMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, ReviewMapper.class);

        job1.setJarByClass(AvgRevNumber.class);
        job1.setReducerClass(JoinReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));

        job1.waitForCompletion(true);

        //end job 1, start job2
        Configuration conf2 = getConf();
        Job job2 = new Job(conf2, "Average reviews number 2nd Job");

        job2.setJarByClass(AvgRevNumber.class);
        job2.setMapperClass(GroupMapper.class);
        job2.setReducerClass(AvgReducer.class);

        FileInputFormat.addInputPath(job2, new Path(args[2]));

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);

        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

        return job2.waitForCompletion(true) ? 0 : 1;
    }

    //delete tmp result
    public static void deleteDir(Path dir) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
        FileSystem  hdfs = FileSystem.get(URI.create("hdfs://quickstart.cloudera/localhost"), conf);
        hdfs.delete(dir, true);
    }

    public static void main(String[] args) throws Exception {

        int res = ToolRunner.run(new Configuration(), new AvgRevNumber(), args);
        deleteDir(new Path(args[2]));
        System.exit(res);
    }
}


