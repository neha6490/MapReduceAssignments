import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

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
public class HW1 {
	
	// A set of active flights active only in 2015
	public static Set<String> activeFlights = new HashSet<String>();
	
	// Custom Object to record the Flights year, month and Average Price
	static class FlightDetails implements Writable{
		
		IntWritable year;
		IntWritable month;
		DoubleWritable avgPrice;
		
		// Default constructor for initialization
		FlightDetails(){
			
			year = new IntWritable();
			month = new IntWritable();
			avgPrice = new DoubleWritable();
		}
		
		// Parameterized Constructor
		FlightDetails(int y, int m, double ap)
		{
			year = new IntWritable(y);
			month = new IntWritable(m);
			avgPrice = new DoubleWritable(ap);
		}
		
		public void write(DataOutput dataoutput) throws IOException
		{
			year.write(dataoutput);
			month.write(dataoutput);
			avgPrice.write(dataoutput);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			year.readFields(in);
			month.readFields(in);
			avgPrice.readFields(in);
		}

		
		
	}	
	//Mapper 
	public static class a2mapper extends Mapper<Object,Text,Text,FlightDetails>{
		
		// Sanity test to confirm sane record
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
								&& origin_wac > 0 && dest_wac > 0)	&&
						(!origin.isEmpty() && !dest.isEmpty() && !origin_cityname.isEmpty() && !dest_cityname.isEmpty()
								&& !origin_state.isEmpty() && !dest_state.isEmpty() && !origin_state_abr.isEmpty() && !dest_state_abr.isEmpty())) {
						
						
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
		
		// Makes sure the time is in the right format
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
		
		
		 
		private Text word = new Text();
		
		// Mapper method
		public void map(Object key,Text value,Context context) throws IOException,InterruptedException			
		{
			String line = value.toString();
			line = line.replaceAll("\"", "");
			String formatedData = line.replaceAll(", ", ":");
			String[] data = formatedData.split(",");
			
			try
			{
				//Only if the data is a sanr record, read it, else skip it
				if(sanityTest(data))
				{
					String carrier = data[8];
					int year = Integer.parseInt(data[0]);
					int month = Integer.parseInt(data[2]);
					double price = Double.parseDouble(data[109]);
					
					if(year == 2015)
						activeFlights.add(carrier);
					
					FlightDetails flight = new FlightDetails(year, month, price); 
					
					
					word.set(carrier);	
					// Set carrier name as key and the flight object as value
					context.write(word, flight);
				}
			}
			catch(NumberFormatException e)
			{
				
			}
	}
		
		}		
	//Reducer
	public static class a2reducer extends Reducer<Text,FlightDetails,Text,Text>{
		private Text result = new Text();
		public void reduce(Text key,Iterable<FlightDetails> flights,Context context) throws IOException,InterruptedException{
			
			double jan =0; double feb=0; double march=0;double apr =0;double may=0;double june=0;double july=0;
			 double aug = 0; double sep=0;double oct=0;double nov=0;double dec = 0;
		     double janC =0;double febC=0;double marchC=0;double aprC=0;double mayC=0;double juneC=0;
		     double julyC=0; double augC=0; double sepC=0;double octC=0;double novC=0;double decC = 0;
		     int count = 0;
			 for (FlightDetails flight : flights) {
				
		        String month = flight.month.toString(); 
		        double avg = Double.parseDouble(flight.avgPrice.toString());
		        count++;
		        // Calculate the sum of each month and increase its frequency
		        switch(month)
		        {
		        case "1" : 
			        {
			        	
			        	jan += avg;
			        	janC++;			        	
			        	break;
			        }
		        case "2" : 
		        {
		        	feb += avg;
		        	febC++;
		        	break;
		        }
		        case "3" : 
		        {
		        	march += avg;
		        	marchC++;
		        	break;
		        }
		        case "4" :
		        {
		        	apr += avg;
		        	aprC++;
		        	break;
		        }
		        case "5" :
		        {
		        	may += avg;
		        	mayC++;
		        	break;
		        }
		        case "6" :
		        {
		        	june += avg;
		        	juneC++;
		        	break;
		        }
		        case "7" :
		        {
		        	july += avg;
		        	julyC++;
		        	break;
		        }
		        case "8" :
		        {
		        	aug += avg;
		        	augC++;
		        	break;
		        }
		        case "9" :
		        {
		        	sep += avg;
		        	sepC++;
		        	break;
		        }
		        case "10" :
		        {
		        	oct += avg;
		        	octC++;
		        	break;
		        }
		        case "11" :
		        {
		        	nov += avg;
		        	novC++;
		        	break;
		        }
		        case "12" :
		        {
		        	dec += avg;
		        	decC++;
		        	break;
		        }
		        }
			 	
			}
			 
			 // Write only flights active in 2015
			 if(activeFlights.contains(key.toString())){
				  if(janC!=0)
				    context.write(key, new Text(new DoubleWritable(jan/janC)+ "\t1\t"+count));
				  if(febC!=0)
			        context.write(key, new Text(new DoubleWritable(feb/febC)+ "\t2\t"+count));
				  if(marchC!=0)
			        context.write(key, new Text(new DoubleWritable(march/marchC)+ "\t3\t"+count));
				  if(aprC!=0)
			        context.write(key, new Text(new DoubleWritable(apr/aprC)+ "\t4\t"+count));
				  if(mayC!=0)
			        context.write(key, new Text(new DoubleWritable(may/mayC)+ "\t5\t"+count));
				  if(juneC!=0)
			        context.write(key, new Text(new DoubleWritable(june/juneC)+ "\t6\t"+count));
				  if(julyC!=0)
			        context.write(key, new Text(new DoubleWritable(july/julyC)+ "\t7\t"+count));
				  if(augC!=0)
			        context.write(key, new Text(new DoubleWritable(aug/augC)+ "\t8\t"+count));
				  if(sepC!=0)
			        context.write(key, new Text(new DoubleWritable(sep/sepC)+ "\t9\t"+count));
				  if(octC!=0)
			        context.write(key, new Text(new DoubleWritable(oct/octC)+ "\t10\t"+count));
				  if(novC!=0)
			        context.write(key, new Text(new DoubleWritable(nov/novC) + "\t11\t"+count));
				  if(decC!=0)
			        context.write(key, new Text(new DoubleWritable(dec/decC)+ "\t12\t"+count));
			 }
			    
		}
	}	
	 public static void main (String[] args) throws InterruptedException,Exception{
		 Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "word count");
		    job.setJarByClass(HW1.class);
		    
		    job.setMapperClass(a2mapper.class);
		    job.setReducerClass(a2reducer.class);

		    //		    job.setCombinerClass(a2reducer.class);
		    
		    //job.setOutputKeyClass(Text.class);
		    //job.setOutputValueClass(FlightDetails.class);
		    
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(FlightDetails.class);
		    
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);

		    
		    
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		 		 
	 }   
}

