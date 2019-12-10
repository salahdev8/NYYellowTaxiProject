package solution;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

  
public class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

 
 // private static DecimalFormat df2 = new DecimalFormat("#.##");
  
  public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		
	    
	  double avg= 0.0  ;
		int count = 0;
		
		double finalavg=0.0 ;
		
		for (DoubleWritable value : values) {
			
	             if (!((Double) value.get()).isInfinite() && !((Double)value.get()).isNaN()) {
			BigDecimal bigDecimal = new BigDecimal(avg+=value.get());
			//double avgearning = bd.doubleValue();
			finalavg = bigDecimal.doubleValue();
			
			
			count++;
		}
		}
		if (count!=0) {
			
	            	 double avg2= (finalavg/count);
	            	
	            	 
	            	// double avgearning= Math.round((avg/count)* 100.0)/100.0;
	            	// double avgearning = bd.doubleValue();
		//double avgearning= Math.round((avg/count)* 100.0)/100.0;
		 			double avgearning = DecimalUtils.round(avg2,2);
		
	
	            	 context.write(key, new DoubleWritable(avgearning));
	
  }
			}
			
}