package solution;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;  
import java.util.Date;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {

  private static DecimalFormat df2 = new DecimalFormat("#.##");

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    
    String line = value.toString();
    String[] data = line.split(",");
    
    String pickupdate = data[1];
    String[] pickupdata =pickupdate.split(" ");
    String dropoffdate = data[2];
    String[] dropoffdata = dropoffdate.split(" ");
    String amntpaid = data[16];
    
    
    
    
    SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    SimpleDateFormat formatter1=new SimpleDateFormat("HH:mm:ss");
    
    
    Date pickuphour = null;
	Date dropoffhour = null;
    
	try {
		
		pickuphour=formatter.parse(pickupdate);
		dropoffhour=formatter.parse(dropoffdate);
	} catch (ParseException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
    
    
	double durmillis =  dropoffhour.getTime()-pickuphour.getTime();
    //convert duration from milliseconds to minutes
    double duration=((durmillis / 1000) / 60) % 60;
    double amountpaid = Double.parseDouble(amntpaid);
    double avgearning = amountpaid/duration;
    
    
	int day = pickuphour.getDay();
	int hour=pickuphour.getHours();
	int hourplus= hour+1;
    String pickup="";
    
    
    switch (day) {
    
    case 1:
        pickup="Monday";
        break;
    case 2:
    	pickup="Tuesday";
        break;
    case 3:
    	pickup="Wednesday";
        break;
    case 4:
    	pickup="Thursday";
        break;
    case 5:
    	pickup="Friday";
        break;
    case 6:
    	pickup="Saturday";
    	break;
    case 7:
    	pickup="Sunday";
        
        
}
    
    if (hour!=23){
    	pickup+="["+hour+","+hourplus+"]";
    }
    
    else if (hour==23){
    	pickup+="[23,0]";
    }
    
    for (String word : line.split(",")) {
      if (word.length() > 0) {
        
        
        context.write(new Text(pickup), new DoubleWritable(avgearning));
      }
    }
  }
}