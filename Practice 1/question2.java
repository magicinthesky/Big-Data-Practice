import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class question2 {

    public static class Map
            extends Mapper<LongWritable, Text, Text, Text> {

        private Text useridpair = new Text(); // type of output key
        private Text friendlist = new Text(); //type of output value


        //first map
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] mydata = value.toString().split("\t");
            if (mydata.length == 2) {
                String userid = mydata[0];
                friendlist.set(mydata[1]);
                String[] outputlist = mydata[1].split(",");
                for (String each : outputlist) {
                    String outputkey = "";
                    if (Integer.parseInt(userid) < Integer.parseInt(each)) {
                        outputkey = userid + "," + each;
                    } else {
                        outputkey = each + "," + userid;
                    }
                    useridpair.set(outputkey);
                    context.write(useridpair, friendlist); //create a pair <useridpair, friendlist>
                }
            }

        }
    }

    //second map output<totalMutualfriendnumber, friendpair>
    public static class Map1
            extends Mapper<Text, Text, LongWritable, Text> {

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(Long.parseLong(value.toString())), key);
        }
    }


    //reduce methods find total number of mutual friends based on the same useridpair
    public static class Reduce
            extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text(); //type of output value

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashSet<String> map = new HashSet<String>();
            int i = 0;
            String[] lines = new String[2];
            int mutualfriends = 0;
            for (Text each : values) {
                lines[i] = each.toString();
                i++;
            }
            if (lines[0] != null && lines[1] != null) {
                String[] first = lines[0].split(",");
                String[] second = lines[1].split(",");
                for (String str : first) {
                    map.add(str);
                }
                for (String each : second) {
                    if (map.contains(each)) {
                        mutualfriends += 1;
                    }
                }
                result.set(Integer.toString(mutualfriends));
                context.write(key, result);
            }
        }

    }

    //reduce methods find total number of mutual friends are within Top 10 in user pairs
    public static class Reduce1
            extends Reducer<LongWritable, Text, Text, LongWritable> {

        private int count = 10;
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value: values){
                if((count--)>0){
                    context.write(value, key);
                }else{
                    return;
                }
            }
        }

    }


    public static void main(String[] args) throws Exception {


        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 2) {
            System.err.println("Usage: Question2 <soc-LiveJournal1Adj.txt> <out>");
            System.exit(2);
        }

        String job_result="/previous";

        // delete old output
        FileSystem hdfs = FileSystem.get(conf);
        if(hdfs.delete(new Path(job_result), true))
            System.out.println("temp output file from previous job is deleted");

        // create a job with name "question1"
        Job job = new Job(conf, "question2");
        job.setJarByClass(question2.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        // set output key type
        job.setOutputKeyClass(Text.class);
        // set output value type
        job.setOutputValueClass(Text.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(job_result));
        //Wait till job completion
        job.waitForCompletion(true);


        if(job.isComplete()){
            Job job1 = new Job(conf, "Topfriends");
            job1.setJarByClass(question2.class);
            job1.setMapperClass(Map1.class);
            job1.setNumReduceTasks(1);
            job1.setReducerClass(Reduce1.class);

            // set output key type
            job1.setMapOutputKeyClass(LongWritable.class);
            job1.setOutputKeyClass(Text.class);
            // set output value type
            job1.setOutputValueClass(Text.class);

            job1.setInputFormatClass(KeyValueTextInputFormat.class);
            job1.setSortComparatorClass(LongWritable.DecreasingComparator.class);

            //set the HDFS path of the input data
            FileInputFormat.addInputPath(job1, new Path(job_result));
            // set the HDFS path for the output
            FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
            //Wait till job completion
            System.exit(job1.waitForCompletion(true) ? 0 : 1);
        }
        else System.exit(1);


    }
}