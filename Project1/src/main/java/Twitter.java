import java.io.*; 

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Twitter {

    public static class FirstJobMap extends Mapper<Object, Text, IntWritable, IntWritable>
    {   
        /* override the parent map */
        @Override 
        public void map(Object key, Text inputLine, Context hadoopContext) throws IOException, InterruptedException 
        {
            /* split the input line (by comma(,)) into user id, follower id */
            String[] inputToken =inputLine.toString().split(","); 
            hadoopContext.write(new IntWritable(Integer.parseInt(inputToken[1])), new IntWritable(Integer.parseInt(inputToken[0]))); 
        }
    }

    public static class SecondJobMap extends Mapper<Object, Text, IntWritable, IntWritable>
    { 
        /*override the parent map */ 
        @Override 
        public void map(Object key, Text inputLine, Context hadoopContext) throws IOException, InterruptedException {

            /* Split the input line (by tab(\t)) into follwer id, count */

            String[] inputToken =inputLine.toString().split("\t"); 
            hadoopContext.write(new IntWritable(Integer.parseInt(inputToken[1])), new IntWritable(1)); 
        }

    }

    public static class FirstJobReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> 
    { 
        /* override the parent reduce */ 
	    @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context hadoopContext) throws IOException, InterruptedException {
            int i =0;

            /* counts the number of occurence of a key */ 
            for(IntWritable value: values) {
                i++;
            }
            /* write the key agaist occurrence */ 
            hadoopContext.write(key, new IntWritable(i));
        }
    }

    public static class SecondJobReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> 
    {  
        /* override the parent reduce */ 
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context hadoopContext) throws IOException, InterruptedException{
	        int total =0;
            for(IntWritable value: values){
                total =total +value.get();
            } 
            hadoopContext.write(key, new IntWritable(total));
        }
    }

    public static void main ( String[] args ) throws Exception 
    {

	    /* first job  */  
        Job Job1 = Job.getInstance(); 
        
        Job1.setJobName("Job-1");
        Job1.setJarByClass(Twitter.class);
        Job1.setOutputKeyClass(IntWritable.class);
        Job1.setOutputValueClass(IntWritable.class);
        Job1.setMapOutputKeyClass(IntWritable.class);
        Job1.setMapOutputValueClass(IntWritable.class);
        Job1.setMapperClass(FirstJobMap.class);
        Job1.setReducerClass(FirstJobReduce.class);
        Job1.setInputFormatClass(TextInputFormat.class);
        Job1.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(Job1,new Path(args[0]));
        FileOutputFormat.setOutputPath(Job1,new Path(args[1]));
        Job1.waitForCompletion(true);

	    /* second job */
        Job Job2 = Job.getInstance();
        Job2.setJobName("Job-2");
        Job2.setJarByClass(Twitter.class);
        Job2.setOutputKeyClass(IntWritable.class);
        Job2.setOutputValueClass(IntWritable.class);
        Job2.setMapOutputKeyClass(IntWritable.class);
        Job2.setMapOutputValueClass(IntWritable.class);
        Job2.setMapperClass(SecondJobMap.class);
        Job2.setReducerClass(SecondJobReduce.class);
        Job2.setInputFormatClass(TextInputFormat.class);
        Job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(Job2,new Path(args[1]));
        FileOutputFormat.setOutputPath(Job2,new Path(args[2]));
        Job2.waitForCompletion(true);
    }
}
