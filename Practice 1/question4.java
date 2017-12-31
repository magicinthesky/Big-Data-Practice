import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class question4 {

    public static class Map
            extends Mapper<LongWritable, Text, Text, Text> {

        HashSet<String> busid=new HashSet<>();
        public void setup(Context context) throws IOException, InterruptedException{
            URI[] file=context.getCacheFiles();
            if(file.length>0){
                String each="";
                String[] filePath = file[0].toString().split("/");
                BufferedReader read=new BufferedReader(new FileReader(filePath[filePath.length-1]));
                each=read.readLine();
                while(each!=null){
                    String[] mydata=each.toString().split("::");
                    if(mydata[1].toLowerCase().contains("palo alto")){
                        busid.add(mydata[0]);
                    }
                    each=read.readLine();
                }
                read.close();
            }
        }

        //output key=user_id and output value=stars from review.csv
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] mydata = value.toString().split("::");
            //if business id is found in review.csv table
            if(busid.contains(mydata[2])){
                context.write(new Text (mydata[1]), new Text(mydata[3]));
            }
        }
    }


    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 3) {
            System.err.println("Usage: Question4 <review.csv> <business.csv> <out>");
            System.exit(2);
        }

        // create a job with name "question4"
        Job job = new Job(conf, "question4");
        job.setJarByClass(question4.class);
        job.setMapperClass(Map.class);
        job.setNumReduceTasks(0);
        //business.csv in cashfile
        job.addCacheFile((new Path(otherArgs[1])).toUri());


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);



    }
}