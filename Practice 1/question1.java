import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class question1 {

    public static class Map
            extends Mapper<LongWritable, Text, Text, Text> {

        private Text useridpair = new Text(); // type of output key
        private Text friendlist = new Text(); //type of output value

        /*
        map method scans input file and output all pairs
        where key=(current userid, one of friend list from current userid)
        and value=(friend list from current userid)
        */
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] mydata = value.toString().split("\t");
            if (mydata.length == 2) {
                String userid = mydata[0]; //current userid
                friendlist.set(mydata[1]); //set friendlist as friend list from current userid
                String[] outputlist = mydata[1].split(","); // get array of friend list from current userid
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

    //reduce methods find mutual friends based on the same useridpair
    public static class Reduce
            extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text(); //type of output value

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashSet<String> map = new HashSet<String>();
            int i = 0;
            String[] lines = new String[2];
            String mutualfriends = "";
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
                        mutualfriends += each + ",";
                    }
                }
                if(!mutualfriends.equals("")){
                    mutualfriends = mutualfriends.substring(0, mutualfriends.length() - 1);
                }
                result.set(mutualfriends);
                //create a pair of <"user1,user2", "list of mutual friends from user1 and user2">
                String keypair=key.toString();
                if(keypair.equals("0,4") || keypair.equals("20,22939") || keypair.equals("1,29826") ||
                        keypair.equals("6222,19272") || keypair.equals("28041,28056")){
                    context.write(key, result);
                }
            }
        }

    }


    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 2) {
            System.err.println("Usage: Question1 <soc-LiveJournal1Adj.txt> <out>");
            System.exit(2);
        }


        // create a job with name "question1"
        Job job = new Job(conf, "question1");
        job.setJarByClass(question1.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        // set output key type
        job.setOutputKeyClass(Text.class);
        // set output value type
        job.setOutputValueClass(Text.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}