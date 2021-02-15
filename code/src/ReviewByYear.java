package project;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.io.Text;
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

public class ReviewByYear extends Configured implements Tool{

    public static class BusinessMapper extends Mapper<LongWritable, Text, Text, ValuePair> {

        JSONParser parser = new JSONParser();
        String businessId;
        String city;

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
            city = (String) jsonObject.get("city");

            //write only if city = Las Vegas, ValuePair is useless.
            if (city.equals("Las Vegas")){
                context.write(new Text(businessId), new ValuePair("Las Vegas", 0.0f) );
            }
        }
    }

    public static class ReviewMapper extends Mapper<LongWritable, Text, Text, ValuePair> {

        JSONParser parser = new JSONParser();
        String businessId;
        double stars;
        String year;

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
            year = ((String) jsonObject.get("date")).substring(0,4);
            stars = (double) jsonObject.get("stars");

            //write to reducer
            context.write(new Text(businessId), new ValuePair(year, stars) );
        }
    }

    public static class GroupMapper extends Mapper<LongWritable, Text, TextPair, FloatWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //read line members: (year, business_id, stars)
            StringTokenizer st = new StringTokenizer(value.toString());
            String business_id = st.nextToken();
            String year = st.nextToken();
            float stars =  Float.parseFloat(st.nextToken());

            context.write(new TextPair(year,business_id), new FloatWritable(stars));

        }
    }

    public static class JoinReducer extends Reducer<Text, ValuePair, Text, ValuePair> {

        boolean isInLasVegas;
        List <ValuePair> reviews = new LinkedList< >();

        @Override
        public void reduce(Text business_id, Iterable<ValuePair> values, Context context) throws IOException, InterruptedException {

            isInLasVegas = false;
            reviews.clear();

            /*
             * iteration on values:
             * separate business from reviews
             */
            for (ValuePair v: values){

                if (v.getFirst().toString().equals("Las Vegas")){
                    // this business is in Las Vegas
                    isInLasVegas = true;
                }
                else{
                    // this is a review: (year, stars)
                    reviews.add(new ValuePair(v.getFirst().toString(), v.getSecond().get()));
                }
            }

            /*
             * proceed only if the reviews are associated to business in Las Vegas
             * Note: the inverse condition (business in Las Vegas without reviews) is implicit.
             *       the reviews list is empty.
             */

            if (isInLasVegas){
                for (ValuePair r : reviews){
                    //business_id, (year, stars)
                    context.write( new Text(business_id), new ValuePair(r.getFirst().toString(), r.getSecond().get()) );
                }
            }
        }
    }

    public static class AvgReducer extends Reducer<TextPair, FloatWritable, Text, FloatWritable> {

        @Override
        public void reduce(TextPair group, Iterable<FloatWritable> stars, Context context) throws IOException, InterruptedException {

            //compute avg for this business in this year
            float sum = 0.0f, tot = 0.0f;

            for (FloatWritable s : stars){
                sum += s.get();
                tot += 1.0f;
            }

            context.write(new Text("(" + group.getFirst() + "," + group.getSecond() + ")"), new FloatWritable(sum/tot));
        }
    }


    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 4) {
            System.err.printf("%s requires four arguments:\n" +
                            "business table path, review table path, path tmp files, output path\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        //start job 1
        Configuration conf1 = getConf();
        Job job1 = new Job(conf1, "Review By Year 1st Job");

        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, BusinessMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, ReviewMapper.class);

        job1.setJarByClass(ReviewByYear.class);
        job1.setReducerClass(JoinReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(ValuePair.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(ValuePair.class);
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));

        job1.waitForCompletion(true);

        //end job 1, start job2
        Configuration conf2 = getConf();
        Job job2 = new Job(conf2, "Review By Year 2nd Job");

        job2.setJarByClass(ReviewByYear.class);
        job2.setMapperClass(GroupMapper.class);
        job2.setReducerClass(AvgReducer.class);

        FileInputFormat.addInputPath(job2, new Path(args[2]));

        job2.setMapOutputKeyClass(TextPair.class);
        job2.setMapOutputValueClass(FloatWritable.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(FloatWritable.class);

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

        int res = ToolRunner.run(new Configuration(), new ReviewByYear(), args);
        deleteDir(new Path(args[2]));
        System.exit(res);
    }
}

