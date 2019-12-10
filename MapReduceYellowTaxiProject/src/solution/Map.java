package solution;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat; 
import java.text.NumberFormat;
import java.util.Date;
import java.util.Calendar;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {


  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	   
	    
	        try {
	            if (key.get() == 0 && value.toString().contains("_") /*Some condition satisfying it is header*/)
	                return;
	            else {

	                String line = value.toString();
	                String[] data = line.split(",");
	                
	                String pickupdate = data[1];
	                String[] pickupdata =pickupdate.split(" ");
	                String dropoffdate = data[2];
	                String[] dropoffdata = dropoffdate.split(" ");
	                String amntpaid = data[16];
	                
	                
	                
	                
	                SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	                

	                SimpleDateFormat myformat= new SimpleDateFormat("EEEE"); // the day of the week spelled out completely
	                
	                
	                Date pickuphour = new Date();
	            	Date dropoffhour = new Date();
	                
	            	try {
	            		
	            		pickuphour=formatter.parse(pickupdate);
	            		dropoffhour=formatter.parse(dropoffdate);
	            	} catch (ParseException e) {
	            		// TODO Auto-generated catch block
	            		e.printStackTrace();
	            	}
	            	
	                
	                
	            	double durmillis =  dropoffhour.getTime()-pickuphour.getTime();
	                //convert duration from milliseconds to minutes
	                double duration=((durmillis / 1000) / 60);
	                NumberFormat f = NumberFormat.getInstance();
	                double amountpaid=0.0;
	                try{
	                amountpaid = f.parse(amntpaid).doubleValue();
	                
	                }
	                catch (ParseException e) {
	            		// TODO Auto-generated catch block
	            		e.printStackTrace();
	            	}
	                double avgearning = amountpaid/duration;
	               
	                
	                
	                Calendar cal=Calendar.getInstance(); 
	                cal.setTime(pickuphour);  
	                int hour = cal.get(Calendar.HOUR_OF_DAY);
	            	
	            	
	            	int hourplus= hour+1;
	            	
	                String pickupday=""; 
	                 pickupday+=myformat.format(pickuphour);
	                
	                

	                
	                if (hour!=23){
	                	pickupday+="["+hour+","+hourplus+"]";
	                }
	                
	                else if (hour==23){
	                	pickupday+="[23,0]";
	                }
	                
	                for (String word : line.split(",")) {
	                  if (word.length() > 0) {
	                    
	                    
	                    context.write(new Text(pickupday), new DoubleWritable(avgearning));
	            }
	                }
	            }
	        }
	        catch (Exception e) {
	            e.printStackTrace();
	        }
	    
    
      }
	        }
    
  
