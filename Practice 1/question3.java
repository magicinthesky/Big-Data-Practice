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

public class question3 {

    public static class PairKey implements WritableComparable<PairKey>{
        private String businessId;
        private Integer src;

        PairKey(){super();}
        PairKey(String businessId, int src){
            this();
            set(businessId,src);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            businessId = in.readUTF();
            src = in.readInt();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(businessId);
            out.writeInt(src);
        }

        public String getId(){
            return this.businessId;
        }

        public Integer getSrc(){
            return this.src;
        }

        public void set(String businessId, int src){
            this.businessId = businessId;
            this.src = src;
        }

        @Override
        public int compareTo(PairKey o) {
            if (this.src != o.getSrc()){
                return this.src - o.getSrc();
            }
            else{
                return this.businessId.compareTo(o.getId());
            }
        }
    }

    public static class PairKeyComparator extends WritableComparator {
        protected PairKeyComparator() {
            super(PairKey.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            PairKey k1 = (PairKey)w1;
            PairKey k2 = (PairKey)w2;

            int result = k1.getId().compareTo(k2.getId());
            if(0 == result) {
                result = k1.getSrc().compareTo(k2.getSrc());
            }
            return result;
        }
    }


    public static class Map
            extends Mapper<LongWritable, Text, Text, FloatWritable> {

        //output key=business_id and output value=stars
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] mydata = value.toString().split("::");
            context.write(new Text(mydata[2]), new FloatWritable(Float.parseFloat(mydata[3])));
        }
    }

    //get output from first job TOp10 business from review.csv: <(businessid,1), avgRating>
    public static class Map2
            extends Mapper<Text, Text, PairKey, Text> {

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new PairKey(key.toString(),1), value);
        }
    }

    //get input from business.csv
    //<businessid,0),businessinfo>
    public static class Map22
            extends Mapper<LongWritable, Text, PairKey, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] mydata = value.toString().split("::");
            String busid=mydata[0];
            String add_cat=mydata[1]+mydata[2];
            context.write(new PairKey(busid,0), new Text(add_cat));
        }
    }


    public static class MyPartition extends Partitioner<PairKey, Text>{
        public int getPartition(PairKey key, Text value, int numPartitions){
                return key.getId().hashCode()%numPartitions;
        }
    }

    public static class Reduce
            extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        private static final TreeMap<Float, List<String>> Top10businesses = new TreeMap<>();
        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float totalstars=0;
            int i=0;
            float average=0;
            for(FloatWritable each: values){
                totalstars=totalstars+each.get();
                i++;
            }
            average=totalstars/i;
            //context.write(key,new FloatWritable(average));
            if(Top10businesses.containsKey(average)){
                Top10businesses.get(average).add(key.toString());
            }else{
                List<String> temp = new ArrayList<>();
                temp.add(key.toString());
                Top10businesses.put(average,new ArrayList<>(temp));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException{
            int count=0;
            for(float each: Top10businesses.descendingKeySet()){
                for(String busid: Top10businesses.get(each)){
                    if (count<10){
                        context.write(new Text(busid), new FloatWritable(each));
                        count++;
                    }else{
                        return;
                    }
                }
            }

        }

    }

    //reduce side join
    public static class Reduce2
            extends Reducer<PairKey, Text, Text, Text> {

        private	String businesstable="";
        public void reduce(PairKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if(key.getSrc()==0) {
                businesstable = values.iterator().next().toString();
            }else{
                context.write(new Text(key.getId()),new Text(businesstable+","+values.iterator().next().toString()));
            }

        }
    }

    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 4) {
            //temp is output address from first job which contains <businessid,avgRating>
            System.err.println("Usage: Question3 <review.csv> <temp> <business.csv> <out>");
            System.exit(2);
        }

        // create a job with name "question3"
        Job job = new Job(conf, "question3");
        job.setJarByClass(question3.class);
        job.setMapperClass(Map.class);
        job.setNumReduceTasks(1);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        job.waitForCompletion(true);

        if(job.isComplete()){
            Job job2 = new Job(conf, "Question3Next");
            job2.setJarByClass(question3.class);

            job2.setMapOutputKeyClass(PairKey.class);
            job2.setMapOutputValueClass(Text.class);

            job2.setPartitionerClass(MyPartition.class);
            job2.setSortComparatorClass(PairKeyComparator.class);

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            MultipleInputs.addInputPath(job2, new Path(otherArgs[1]), KeyValueTextInputFormat.class, Map2.class);
            MultipleInputs.addInputPath(job2, new Path(otherArgs[2]), TextInputFormat.class, Map22.class);

            job2.setReducerClass(Reduce2.class);
            FileOutputFormat.setOutputPath(job2, new Path(otherArgs[3]));
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }else System.exit(1);



    }
}