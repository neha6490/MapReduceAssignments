import java.io.BufferedReader; 
import java.io.BufferedWriter; 
import java.io.DataInput; 
import java.io.DataOutput; 
import java.io.File; 
import java.io.FileInputStream; 
import java.io.FileNotFoundException; 
import java.io.IOException; 
import java.io.InputStreamReader; 
import java.util.ArrayList; 
import java.util.HashMap; 
import java.util.HashSet; 
import java.util.Iterator; 
import java.util.List; 
import java.util.Map; 
import java.util.Random; 
import java.util.Set; 
import java.util.TreeMap; 
import java.util.Collections; 
import java.util.zip.GZIPInputStream; 
import java.io.FileWriter; 
import java.io.IOException; 
import java.io.BufferedWriter; 
 
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.DoubleWritable; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.io.Writable; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.Mapper; 
import org.apache.hadoop.mapreduce.Reducer; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 

/*
 * Authors: Neha Patwardhan, Joy Machado
 * */ 

public class HW4
{
    
    // Flights active in Jan 2015 for map reduce program
    public static Set<String> activeFlights = new HashSet<String>();
    public static Set<Integer> years = new HashSet<Integer>();
      
    // Convert time in the right format for all inputs
    public static int  timeToMinute(int time) { 
        String timeVal = String.valueOf(time); 
        StringBuilder sb = new StringBuilder(timeVal); 
        int len = timeVal.length(); 
        int zeros = 4 - len; 
        StringBuilder z = new StringBuilder(); 
        while (zeros != 0) { 
            z.append('0'); 
            zeros--; 
        } 
        String add = z.toString(); 
        sb.insert(0, add); 
        String correct_format = sb.toString(); 
        int minutes = Integer.valueOf(correct_format.substring(0, 2))*60 + Integer.valueOf(correct_format.substring(2, 4)); 
        return minutes; 
    } 
     
    // Sanity Test
    public static boolean sanityTest(String data[]) 
    { 
        boolean validData = true; 
         
        if(data.length != 110) 
            validData = false; 
        try 
        { 
            int CRSArrTime = Integer.valueOf(data[40]); 
            int CRSDepTime = Integer.valueOf(data[29]); 
            int timeZone = timeToMinute(Integer.valueOf(data[40])) - timeToMinute(Integer.valueOf(data[29])) - Integer.valueOf(data[50]); 
            int origin_AirportID = Integer.valueOf(data[11]); 
            int dest_AirportID = Integer.valueOf(data[20]); 
            int origin_AirportSeqID = Integer.valueOf(data[12]); 
            int dest_AirportSeqID = Integer.valueOf(data[21]); 
            int origin_CityMarketID = Integer.valueOf(data[13]); 
            int dest_CityMarketID = Integer.valueOf(data[22]); 
            int origin_StateFips = Integer.valueOf(data[17]); 
            int dest_StateFips = Integer.valueOf(data[26]); 
            int origin_wac = Integer.valueOf(data[19]); 
            int dest_wac = Integer.valueOf(data[28]); 
            int cancelled = Integer.valueOf(data[47]); 
            String origin = data[14]; 
            String dest = data[23]; 
            String origin_cityname = data[15]; 
            String dest_cityname = data[24]; 
            String origin_state = data[18]; 
            String dest_state = data[27]; 
            String origin_state_abr = data[16]; 
            String dest_state_abr = data[25]; 
             
            if ((CRSArrTime != 0 && CRSDepTime != 0) && 
                    (timeZone%60 == 0) &&     
                    (origin_AirportID >0 && dest_AirportID > 0 && origin_AirportSeqID > 0 && dest_AirportSeqID > 0  
                            && origin_CityMarketID > 0 && dest_CityMarketID > 0 && origin_StateFips > 0 && dest_StateFips > 0  
                            && origin_wac > 0 && dest_wac > 0)    && 
                    (!origin.isEmpty() && !dest.isEmpty() && !origin_cityname.isEmpty() && !dest_cityname.isEmpty() 
                            && !origin_state.isEmpty() && !dest_state.isEmpty() && !origin_state_abr.isEmpty() && !dest_state_abr.isEmpty())) { 
                     
                    // for flights which are not cancelled 
                    if (cancelled != 1) { 
                        if (((timeToMinute(Integer.valueOf(data[41])) - timeToMinute(Integer.valueOf(data[30])) - Integer.valueOf(data[51]) - timeZone)/60)%24 == 0) { 
                            if (Float.valueOf(data[42]) > 0) { 
                                if (Float.valueOf(data[42]).equals(Float.valueOf(data[43]))) 
                                    validData = true; 
                                else 
                                return false; 
                            } 
                             
                            if (Float.valueOf(data[42]) < 0) { 
                                if (Float.valueOf(data[43]) == 0) 
                                    validData = true; 
                                else 
                                    return false; 
                            } 
                             
                            if (Float.valueOf(data[43]) >= 15) { 
                                if (Float.valueOf(data[44]) == 1) 
                                    validData = true; 
                                else 
                                    return false; 
                            } 
                        } else 
                            return false; 
                    } 
                } else { 
                    return false; 
                } 
        } 
        catch(Exception e) 
        { 
            return false; 
        } 
         
        return validData; 
    } 
     
     
   // Custom object to store values for a carrier for map reduce analysis  
    static class FlightDetails implements Writable 
    { 
         
        IntWritable time; 
        IntWritable distance; 
        DoubleWritable avgPrice; 
         
        FlightDetails(){ 
             
            time = new IntWritable(); 
            distance = new IntWritable(); 
            avgPrice = new DoubleWritable(); 
        } 
         
        FlightDetails(int t, int d, double ap) 
        { 
            time = new IntWritable(t); 
            distance = new IntWritable(d); 
            avgPrice = new DoubleWritable(ap); 
        } 
         
        public void write(DataOutput dataoutput) throws IOException 
        { 
            time.write(dataoutput); 
            distance.write(dataoutput); 
            avgPrice.write(dataoutput); 
        } 
 
        @Override 
        public void readFields(DataInput in) throws IOException { 
            time.readFields(in); 
            distance.readFields(in); 
            avgPrice.readFields(in); 
        } 
 
         
         
    } 
    
    // Mapper Class
    public static class a2mapper extends Mapper<Object,Text,Text,FlightDetails> 
    { 
         
          
        private Text word = new Text(); 
        public void map(Object key,Text value,Context context) throws IOException,InterruptedException             
        { 
            String line = value.toString(); 
            line = line.replaceAll("\"", ""); 
            String formatedData = line.replaceAll(", ", ":"); 
            String[] data = formatedData.split(","); 
            double median = 0.0; 
            try 
            { 
                if(sanityTest(data)) 
                { 
                    String carrier = data[8]; 
                    double price = Double.parseDouble(data[109]); 

                    int time = Integer.parseInt(data[52]);
		    int year = Integer.parseInt(data[0]);
                    int distance = Integer.parseInt(data[54]);
      		    
		    // Prepare a list of carriers active in 2015
                    if(year == 2015)
                    { 
                    	activeFlights.add(carrier);
                    }
            
                    FlightDetails flight = new FlightDetails(time, distance, price);  
                    
                    word.set(carrier);  

   		    // Send flight values to the reducer only if its between 2010 to 2014
                    if(years.contains(year)) {
			context.write(word, flight); 
			
			}
                    			
                } 
            } 
            catch(NumberFormatException e) 
            { 
               
            } 
    } 
         
}     
    // Reducer class
    public static class meanReducer extends Reducer<Text,FlightDetails,Text,Text> 
        { 
             
            public void reduce(Text key,Iterable<FlightDetails> flights,Context context) throws IOException,InterruptedException 
            { 
		
            	for(FlightDetails flight : flights)
            	{
			// Write to output only values present in 2015
            		if(activeFlights.contains(key.toString()))
                		{
            				IntWritable time = flight.time;
            				IntWritable distance = flight.distance;
            				DoubleWritable avgPrice = flight.avgPrice;
                			context.write(key, new Text(time + "\t" + distance + "\t" + avgPrice));
                		}
            	}
            } 
            
        } 
     
     
       
    // Main method   
     public static void main (String[] args) throws InterruptedException,Exception 
     {    
    	 	years.add(2010);
    		years.add(2011);
    	 	years.add(2012);
    	 	years.add(2013);
    	 	years.add(2014);
    	 		 
                 Configuration conf = new Configuration(); 
                 Job job = Job.getInstance(conf, "word count"); 
                 job.setJarByClass(HW4.class); 
                  
                 job.setMapperClass(a2mapper.class); 
                           
                 job.setReducerClass(meanReducer.class); 
                 job.setMapOutputKeyClass(Text.class); 
                 job.setMapOutputValueClass(FlightDetails.class); 
                     
                 job.setOutputKeyClass(Text.class); 
                 job.setOutputValueClass(Text.class); 
 
                   
                FileInputFormat.addInputPath(job, new Path(args[0])); 
                FileOutputFormat.setOutputPath(job, new Path(args[1])); 
                if(job.waitForCompletion(true))
                { 
                        System.exit(0); 
                } 
      } 
     
}
             
             
    
