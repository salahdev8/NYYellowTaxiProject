package solution;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class Driver {
	
	public static void main(String[] args) throws Exception {

	    
	    if (args.length != 2) {
	      System.out.printf(
	          "Usage: Driver <input dir> <output dir>\n");
	      System.exit(-1);
	    }

	   
	    Job job = new Job();
	    
	    
	    job.setJarByClass(Driver.class);
	    
	    
	    job.setJobName("Average earning");

	    
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

	    
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);

	    

	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DoubleWritable.class);

	    

	    boolean success = job.waitForCompletion(true);
	    System.exit(success ? 0 : 1);
	  }
	}