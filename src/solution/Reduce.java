package solution;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.DecimalFormat;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

  
public class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

 
  private static DecimalFormat df2 = new DecimalFormat("#.##");
  
  public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		
	    double avg = 0.0;
		int count = 0;
		
		
		
		for (DoubleWritable value : values) {
			
			
			avg += value.get();
			count++;
		}
		
		
		double avg2= (avg/count);
		
		double avgearning = DecimalUtils.round(avg2,2);
	
		context.write(key, new DoubleWritable(avgearning));
	
  }
}