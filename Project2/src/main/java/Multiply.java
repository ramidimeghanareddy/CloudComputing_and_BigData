import java.io.*;
import java.util.*; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

class Tripple implements Writable

{
    public short t;  
    public int i;  public double v; 

    Tripple() 

    {
        t =(short)0;
        i =0;
        v =0.0;
    }

    Tripple(short t, int i, double v)

    { 
        this.t =t; 
        this.i =i; 
        this.v =v;
    }

    @Override
    public void readFields(DataInput in)  throws IOException

    { 
        this.t =in.readShort(); 
        this.i =in.readInt(); 
        this.v =in.readDouble(); 
    }

    @Override
    public void write(DataOutput out) throws IOException

    { 
        out.writeShort(this.t); 
        out.writeInt(this.i); 
        out.writeDouble(this.v); 
    }
}

class Pair implements WritableComparable<Pair>

{
    public int num1, num2;

    Pair () { }

    Pair ( int num1, int num2) 
    
    {
        this.num1 = num1; 
        this.num2 = num2;
    }

    @Override
    public void readFields(DataInput in) throws IOException
    
    { 
        num1 =in.readInt(); 
        num2 =in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException
    
    { 
        out.writeInt(num1);  
        out.writeInt(num2); 
    }

    @Override
    public int compareTo(Pair pair)
    
    {  
        int compVal =0; 
        if(num1 >pair.num1)
        
        {
            compVal =1; 

        }

        else if(num1 <pair.num1) 

        {
            compVal =-1; 

        }
        
        else
        
        {
            if(num2 >pair.num2)
            
            {
                compVal =1;
            }
            else if(num2 <pair.num2)
            
            {
                compVal =-1;

            }
        }

        return compVal;
    }

    @Override
    public String toString() 
    
    { 
        return num1 + "," + num2;
    
    }

}

public class Multiply extends Configured implements Tool

{  
    public static class Job1MapM extends Mapper<Object, Text, IntWritable, Tripple>
    
    {  

        @Override
        public void  map(Object key, Text line, Context instance) throws IOException, InterruptedException 
        
        { 
            String[] splitStrInputToken =line.toString().split(","); 
            int i =Integer.parseInt(splitStrInputToken[0]);
            int k =Integer.parseInt(splitStrInputToken[1]);
            double m =Double.parseDouble(splitStrInputToken[2]);
            instance.write(new IntWritable(k), new Tripple((short)0, i, m)); 
        }
    } 
    
    public static class Job1MapN extends Mapper<Object, Text, IntWritable, Tripple>
    
    { 

        @Override
        public void map(Object key, Text line, Context instance)  throws IOException, InterruptedException
        
        {   
            String[] splitStrInputToken =line.toString().split(",");  
            int k = Integer.parseInt(splitStrInputToken[0]); 
            int j = Integer.parseInt(splitStrInputToken[1]);
            double n =Double.parseDouble(splitStrInputToken[2]);  
            instance.write(new IntWritable(k), new Tripple((short)1, j, n));
        }
    } 

    public static class Job1Reduce extends Reducer<IntWritable, Tripple, Pair, DoubleWritable>

    { 

        @Override
        public void reduce(IntWritable k, Iterable<Tripple> values, Context instance) throws IOException, InterruptedException 
        
        {

            ArrayList<Tripple> M_values =new ArrayList<Tripple>(); 
            ArrayList<Tripple> N_values =new ArrayList<Tripple>(); 
            Configuration instanceConfig = instance.getConfiguration();
 
            for(Tripple val: values) 
            
            { 
                Tripple tripple = ReflectionUtils.newInstance(Tripple.class, instanceConfig);
			    ReflectionUtils.copy(instanceConfig, val, tripple);
 
                if(tripple.t ==0) 
                
                {M_values.add(tripple);}

                else if (tripple.t ==1)
                
                {N_values.add(tripple);}  
            }  

            for(int i =0; i <M_values.size(); i++)
            
            { 
                for(int j =0; j <N_values.size(); j++) 
                
                { 
                    int num1 =M_values.get(i).i , num2 =N_values.get(j).i; 
                    double mxn =M_values.get(i).v *N_values.get(j).v;

                    instance.write(new Pair(num1 ,num2), new DoubleWritable(mxn)); 
                } 
            }
        }
    } 
 
    public static class Job2Map extends Mapper<Object, Text, Pair, DoubleWritable>
    
    {
		@Override
		public void map(Object key, Text line, Context context)  throws 
        IOException, InterruptedException {  
			String[] splitStrInputToken =line.toString().split("\t"); 
            String[] pair =splitStrInputToken[0].split(",");
            double val =Double.parseDouble(splitStrInputToken[1]);  
			context.write(new Pair(Integer.parseInt(pair[0]), Integer.parseInt(pair[1])), new DoubleWritable(val));
		}
	}
	
	public static class Job2Reduce extends Reducer<Pair, DoubleWritable, Pair, DoubleWritable> {
		@Override
		public void reduce(Pair key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			double sum = 0.0;
			for(DoubleWritable value : values)  {
                sum += value.get(); 
            }; 
            context.write(key, new DoubleWritable(sum));
		}
	}

    public int run(String[] args) throws Exception 
    
    {
        Configuration cfg =new Configuration();
        cfg.set("mapreduce.output.textoutputformat.separator", ",");
        
        Job Job1 = Job.getInstance();
		Job1.setJobName("arrange-matrix-job");
		Job1.setJarByClass(Multiply.class);

		MultipleInputs.addInputPath(Job1, new Path(args[0]), TextInputFormat.class, Job1MapM.class);
		MultipleInputs.addInputPath(Job1, new Path(args[1]), TextInputFormat.class, Job1MapN.class);
		Job1.setReducerClass(Job1Reduce.class);
		
		Job1.setMapOutputKeyClass(IntWritable.class);
		Job1.setMapOutputValueClass(Tripple.class);
		
		Job1.setOutputKeyClass(Pair.class);
		Job1.setOutputValueClass(DoubleWritable.class);
		
		Job1.setOutputFormatClass(TextOutputFormat.class);
		
		FileOutputFormat.setOutputPath(Job1, new Path(args[2])); 
		Job1.waitForCompletion(true); 

        
        Job Job2 = Job.getInstance(cfg);
		Job2.setJobName("Job2");
		Job2.setJarByClass(Multiply.class);
		
		Job2.setMapperClass(Job2Map.class);
		Job2.setReducerClass(Job2Reduce.class);
		
		Job2.setMapOutputKeyClass(Pair.class);
		Job2.setMapOutputValueClass(DoubleWritable.class);
		
		Job2.setOutputKeyClass(Pair.class);
		Job2.setOutputValueClass(DoubleWritable.class);
		
		Job2.setInputFormatClass(TextInputFormat.class);
		Job2.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(Job2, new Path(args[2]));
		FileOutputFormat.setOutputPath(Job2, new Path(args[3]));
		
		Job2.waitForCompletion(true);
        return 0; 
    }

    public static void main ( String[] args ) throws Exception {  
        ToolRunner.run(new Configuration(), new Multiply(), args);
    }
}
