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
/*Authors: Neha Patwardhan, Joy Machado*/ 
public class HW1
{ 
    // Path for outputs
    public static String outputPath = "/home/neha/test/outputs/";
    
    // Flights active in Jan 2015 for map reduce program
    public static Set<String> activeFlights = new HashSet<String>();
    
    // Flights active in Jan 2015 for Sequential program
    public static Set<String> activeFlightsSeq = new HashSet<String>();
    
    // Flights active in Jan 2015 for Multi threaded program
    public static Set<String> activeFlightsMT = new HashSet<String>(); 
    
    // Referred from Cory Hardman's solution to find median in linear time
    public static < T extends Comparable > T quickSelect(List < T > values, int k) 
    { 
        int left = 0; 
        int right = values.size() - 1; 
        Random rand = new Random(); 
        while(true) 
        { 
            int partionIndex = rand.nextInt(right - left + 1) + left; 
            int newIndex = partition(values, left, right, partionIndex); 
            int q = newIndex - left + 1; 
            if(k == q) 
            { 
                return values.get(newIndex); 
            } 
            else if(k < q) 
            { 
                right = newIndex - 1; 
            } 
            else 
            { 
                k -= q; 
                left = newIndex + 1; 
            } 
        } 
    } 
     
    private static < T extends Comparable> int partition(List < T > values, int left, int right, int partitionIndex) 
    { 
        T partionValue = values.get(partitionIndex); 
        int newIndex = left; 
        T temp = values.get(partitionIndex); 
        values.set(partitionIndex, values.get(right)); 
        values.set(right, temp); 
        for(int i = left; i < right; i++) 
        { 
            if(values.get(i).compareTo(partionValue) < 0) 
            { 
                temp = values.get(i); 
                values.set(i, values.get(newIndex)); 
                values.set(newIndex, temp); 
                newIndex++; 
            } 
        } 
        temp = values.get(right); 
        values.set(right, values.get(newIndex)); 
        values.set(newIndex, temp); 
        return newIndex; 
    } 
     
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
     
    // Custom object to store values for a carrier for the sequential analysis
    static class SeqFlight 
    { 
        String carrier; 
        int year; 
        int month; 
        ArrayList<Double> priceList = new ArrayList<Double>(); 
         
        SeqFlight() 
        { 
             
        } 
         
        SeqFlight(String carr, int y, int m) 
        { 
            carrier = carr; 
            year = y; 
            month = m; 
        } 
    } 
     
   // Custom object to store values for a carrier for map reduce analysis  
    static class FlightDetails implements Writable 
    { 
         
        IntWritable year; 
        IntWritable month; 
        DoubleWritable avgPrice; 
         
        FlightDetails(){ 
             
            year = new IntWritable(); 
            month = new IntWritable(); 
            avgPrice = new DoubleWritable(); 
        } 
         
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
    
    // Mapper
    public static class a2mapper extends Mapper<Object,Text,Text,FlightDetails> 
    { 
         
         /*private final static IntWritable one = new IntWritable(1);*/ 
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
                    int year = Integer.parseInt(data[0]); 
                    int month = Integer.parseInt(data[2]); 
                    double price = Double.parseDouble(data[109]); 
                    //System.out.println(year); 
                    if(year == 2015){ 
                    //System.out.println(carrier); 
                    activeFlights.add(carrier);} 
                         
                     
                    FlightDetails flight = new FlightDetails(year, month, price);  
                     
                     
                    word.set(carrier);     
                    //System.out.println("Try"); 
                    context.write(word, flight); 
                } 
            } 
            catch(NumberFormatException e) 
            { 
               
            } 
    } 
         
}     
    // Reducer to compute mean
    public static class meanReducer extends Reducer<Text,FlightDetails,Text,Text> 
        { 
            private Text result = new Text(); 
            public void reduce(Text key,Iterable<FlightDetails> flights,Context context) throws IOException,InterruptedException 
            { 
                int count = 0; 
                double median = 0.0; 
                double sum = 0; 
                TreeMap<String, ArrayList<Double>> months = new TreeMap<String, ArrayList<Double>>();
                
                // Add flights per month
                 for (FlightDetails flight : flights) 
                 {  
                    String month = flight.month.toString();  
                    double avg = Double.parseDouble(flight.avgPrice.toString()); 
                    count++; 
                     
                    if(months.containsKey(month)) 
                    { 
                        ArrayList list = months.get(month); 
                        list.add(avg); 
                        months.put(month, list); 
                    } 
                    else 
                    {   ArrayList list = new ArrayList(); 
                        list.add(avg); 
                        months.put(month, list); 
                    } 
                     
                     
             } 
                  // Calculate mean for each month
                 for (String month1 : months.keySet()) 
                    { 
                         
                        for(double avg1 : months.get(month1)) 
                        { 
                            sum += avg1; 
                             
                        } 
                         
                       ArrayList list = months.get(month1); 
                          Collections.sort(list); 
                       if(list.size() %2 !=0 ) 
                           median = (double) list.get(list.size()/2); 
                           else 
                               median = (double) ((double)list.get(list.size()/2 -1) + (double)list.get(list.size()/2)) /2; 
                       sum = sum/list.size(); 
                       
                       // Check for active flights
                       if(activeFlights.contains(key.toString())) 
                        { 
                         context.write(new Text(month1), new Text(key+"\t"+sum)); 
                        } 
 
                    } 
            } 
        } 
     
     
    // Reducer to find median
    public static class medianReducer extends Reducer<Text,FlightDetails,Text,Text> 
    { 
        private Text result = new Text(); 
        public void reduce(Text key,Iterable<FlightDetails> flights,Context context) throws IOException,InterruptedException 
        { 
            int count = 0; 
            double median = 0.0; 
            TreeMap<String, ArrayList<Double>> months = new TreeMap<String, ArrayList<Double>>(); 
             for (FlightDetails flight : flights) 
             { 
                //System.out.println("Reducer double writable val: "+flight.avgPrice); 
                String month = flight.month.toString();  
                double avg = Double.parseDouble(flight.avgPrice.toString()); 
                count++; 
                 
                if(months.containsKey(month)) 
                { 
                    ArrayList list = months.get(month); 
                    list.add(avg); 
                    months.put(month, list); 
                } 
                else 
                {   ArrayList list = new ArrayList(); 
                    list.add(avg); 
                    months.put(month, list); 
                } 
                 
                 
         } 
             // Calculate median for each month
             for (String month1 : months.keySet()) 
                { 
                   ArrayList list = months.get(month1); 
                      Collections.sort(list); 
                   if(list.size() %2 !=0 ) 
                       median = (double) list.get(list.size()/2); 
                       else 
                           median = (double) ((double)list.get(list.size()/2 -1) + (double)list.get(list.size()/2)) /2; 
                    
                   if(activeFlights.contains(key.toString())) 
                    { 
                       context.write(new Text(month1), new Text(key+"\t"+median)); 
                    } 
 
                } 
        } 
    } 
     
    // Reducer to find Fast Median values
    public static class fastMedianReducer extends Reducer<Text,FlightDetails,Text,Text> 
    { 
        private Text result = new Text(); 
        public void reduce(Text key,Iterable<FlightDetails> flights,Context context) throws IOException,InterruptedException 
        { 
            int count = 0; 
            double median = 0.0; 
            TreeMap<String, ArrayList<Double>> months = new TreeMap<String, ArrayList<Double>>(); 
             for (FlightDetails flight : flights) 
             { 
                //System.out.println("Reducer double writable val: "+flight.avgPrice); 
                String month = flight.month.toString();  
                double avg = Double.parseDouble(flight.avgPrice.toString()); 
                count++; 
                 
                if(months.containsKey(month)) 
                { 
                    ArrayList list = months.get(month); 
                    list.add(avg); 
                    months.put(month, list); 
                } 
                else 
                {   ArrayList list = new ArrayList(); 
                    list.add(avg); 
                    months.put(month, list); 
                } 
                 
                 
         } 
             // Calculate fast median for each month
             for (String month1 : months.keySet()) 
                { 
                  
                   ArrayList<Double> avgPrices = (ArrayList)months.get(month1); 
                   double fastMedian = quickSelect(avgPrices, avgPrices.size()/2); 
                   if(activeFlights.contains(key.toString())) 
                    { 
                       context.write(new Text(month1), new Text(key+"\t"+fastMedian)); 
                    } 
 
                } 
        } 
    } 
     
     
     
     
     
    // Main      
     public static void main (String[] args) throws InterruptedException,Exception 
     {    
    	 
         long startTime = 0; 
         long endTime = 0; 
         long totalTime= 0; 
          
            HW1 hw1 = new HW1(); 
            
             switch(args[2]){ 
             // Run the sequential program
             case ("sequential") : 
             { 
                 startTime = System.currentTimeMillis(); 
                 Sequential seq = hw1.new Sequential(); 
                 TreeMap seqMap = seq.readSeqData(args[0]); 
                 switch(args[3]) 
                 { 
                     case "mean":  
                     { 
                         seq.meanSeqThread(seqMap); 
                         break; 
                     } 
                     case "median":  
                     { 
                         seq.medianSeq(seqMap); 
                         break; 
                     } 
                     case "fast median": 
                     { 
                         seq.medianFastSeq(seqMap); 
                         break; 
                     } 
                      
                 } 
                 endTime = System.currentTimeMillis(); 
                 totalTime = endTime - startTime; 
                 FileWriter fstream = new FileWriter(outputPath+args[2]+".csv",true); 
                 fstream.append(args[2]+","+args[3]+","+totalTime); 
                 fstream.append("\n"); 
                 fstream.flush(); 
                 fstream.close();               
               
                 break; 
             } 
             
             // Run the multi threaded program
             case "multithreaded" : 
             { 
                 startTime = System.currentTimeMillis(); 
                 MultiThreaded mt = hw1.new MultiThreaded(); 
                 TreeMap mtMap = mt.readFile(args[0]); 
                 switch(args[3]) 
                 { 
                     case "mean":  
                      { 
                          mt.meanMultiThread(mtMap); 
                          break; 
                      } 
                      case "median":  
                      { 
                          mt.medianMultiThreaded(mtMap); 
                          break; 
                      } 
                      case "fast median": 
                      { 
                          mt.medianFastMultiThreaded(mtMap); 
                          break; 
                      } 
                 } 
                 endTime = System.currentTimeMillis(); 
                 totalTime = endTime - startTime; 
                 FileWriter fstream = new FileWriter(outputPath+args[2]+".csv",true); 
                 fstream.append(args[2]+","+args[3]+","+totalTime); 
                 fstream.append("\n"); 
                 fstream.flush(); 
                 fstream.close();               
               
                 break; 
             } 
             
             // Run the pseudo distributed  mode
             case "pseudo"  : 
             {     
                 startTime = System.currentTimeMillis(); 
                 Configuration conf = new Configuration(); 
                 Job job = Job.getInstance(conf, "word count"); 
                 job.setJarByClass(HW1.class); 
                  
                 job.setMapperClass(a2mapper.class); 
                 switch(args[3]) 
                 { 
                  
                    case "mean":  
                      { 
                           
                        job.setReducerClass(meanReducer.class); 
                        break; 
                      } 
                      case "median":  
                      { 
                          job.setReducerClass(medianReducer.class); 
                          break; 
                      } 
                      case "fast median": 
                      { 
                          job.setReducerClass(fastMedianReducer.class); 
                          break; 
                      } 
                 } 
                      job.setMapOutputKeyClass(Text.class); 
                    job.setMapOutputValueClass(FlightDetails.class); 
                     
                    job.setOutputKeyClass(Text.class); 
                    job.setOutputValueClass(Text.class); 
 
                   
                    FileInputFormat.addInputPath(job, new Path(args[0])); 
                    FileOutputFormat.setOutputPath(job, new Path(args[1])); 
                    if(job.waitForCompletion(true))
                    { 
                        endTime = System.currentTimeMillis(); 
                        totalTime = endTime - startTime; 
			            FileWriter fstream = new FileWriter(outputPath+"pseudo.csv",true); 
			            fstream.append(args[2]+","+args[3]+","+totalTime); 
			            fstream.append("\n"); 
			            fstream.flush(); 
			            fstream.close(); 
			            System.exit(0); 
                    } 
             }  
             
             // Run the EMR mode
             case "emr": 
             { 
                 startTime = System.currentTimeMillis(); 
                 Configuration conf = new Configuration(); 
                 Job job = Job.getInstance(conf, "word count"); 
                 job.setJarByClass(HW1.class); 
                  
                 job.setMapperClass(a2mapper.class); 
                 switch(args[3]) 
                 { 
                  
                    case "mean":  
                      { 
                           
                        job.setReducerClass(meanReducer.class); 
                        break; 
                      } 
                      case "median":  
                      { 
                          job.setReducerClass(medianReducer.class); 
                          break; 
                      } 
                      case "fast median": 
                      { 
                          job.setReducerClass(fastMedianReducer.class); 
                          break; 
                      } 
                 } 
	                   job.setMapOutputKeyClass(Text.class); 
	                   job.setMapOutputValueClass(FlightDetails.class); 
	                     
	                   job.setOutputKeyClass(Text.class); 
	                   job.setOutputValueClass(Text.class); 
	 
	                   
	                  FileInputFormat.addInputPath(job, new Path(args[0])); 
	                  FileOutputFormat.setOutputPath(job, new Path(args[1])); 
	                    if(job.waitForCompletion(true))
	                    { 
	                        endTime = System.currentTimeMillis(); 
	                        totalTime = endTime - startTime; 
	                        FileWriter fstream = new FileWriter(outputPath+"emr.csv",true); 
	                        fstream.append(args[2]+","+args[3]+","+totalTime); 
	                        fstream.append("\n"); 
	                        fstream.flush(); 
	                        fstream.close(); 
	                        System.exit(0); 
	                     } 
                
             } 
                
             }
         
     } 
      
     // Class for sequential analysis
     public class Sequential 
     { 
    	 	// Read the files
            public TreeMap readSeqData(String dir) throws NumberFormatException, IOException 
            { 
                TreeMap<String, ArrayList<Double>> map = new TreeMap<String, ArrayList<Double>>(); 
                File myDir = new File(dir); 
                File[] fileNames = myDir.listFiles(); 
                for(File fileName : fileNames) 
                { 
                    FileInputStream inputStream = new FileInputStream(fileName); 
                    GZIPInputStream gzip = null; 
                try  
                { 
                    gzip = new GZIPInputStream(new FileInputStream(fileName)); 
                } 
                catch (FileNotFoundException e)  
                { 
                    e.printStackTrace(); 
                }  
                catch (IOException e) 
                { 
                    e.printStackTrace(); 
                } 
                BufferedReader br = new BufferedReader(new InputStreamReader(gzip)); 
                String content; 
                
                // read each line
                while ((content = br.readLine()) != null) 
                { 
                    String line = content.toString(); 
                    line = line.replaceAll("\"", ""); 
                    String formatedData = line.replaceAll(", ", ":"); 
                    String[] data = formatedData.split(","); 
                    boolean sanityTest = sanityTest(data);     
                    if(sanityTest) 
                    { 
                    String carrier = data[8]; 
                    int year = Integer.parseInt(data[0]); 
                    int month = Integer.parseInt(data[2]); 
                    double price = Double.parseDouble(data[109]); 
                    if(year == 2015) 
                    { 
                        activeFlightsSeq.add(carrier); 
                    } 
                    
                    String key = carrier+"_"+month; 
                    if(map.containsKey(key)) 
                    { 
                        ArrayList<Double> list = map.get(key); 
                        list.add(price); 
                        map.put(key, list); 
                    } 
                    else 
                    { 
                        ArrayList<Double> list = new ArrayList(); 
                        list.add(price); 
                        map.put(key,list); 
                    } 
                    } 
                } 
                } 
                return map; 
            } 
             
             // Calculate the mean for sequential analysis
            public void meanSeqThread(TreeMap<String, ArrayList<Double>> seqMap)throws IOException 
            { 
                FileWriter fstream = new FileWriter(outputPath+"Mean Sequentail.txt"); 
                BufferedWriter out = new BufferedWriter(fstream); 
                Iterator it = seqMap.entrySet().iterator(); 
                while (it.hasNext()) 
                { 
                    Map.Entry pair = (Map.Entry)it.next(); 
                     
                    ArrayList<Double> avgPrices = (ArrayList)pair.getValue(); 
                    int sum = 0; 
                    for(double price : avgPrices) 
                    { 
                        sum+=price; 
                    } 
                    sum = sum/avgPrices.size(); 
                    String key = (String)pair.getKey(); 
                    String carrier = key.substring(0,2); 
                    String month = key.substring(3,key.length()); 
                    if(activeFlightsSeq.contains(carrier)) 
                    { 
                    out.write(carrier+"\t"+month+"\t"+sum); 
                    out.newLine(); 
                    System.out.println(carrier+"\t"+month+"\t"+sum); 
                    } 
                    it.remove();  
                } 
                out.close(); 
            } 
             
            // Calculate the median for sequential analysis
            public void medianSeq(TreeMap<String, ArrayList<Double>> seqMap)throws IOException 
            { 
                FileWriter fstream = new FileWriter(outputPath+"Median Sequential.txt"); 
                BufferedWriter out = new BufferedWriter(fstream); 
                Iterator it = seqMap.entrySet().iterator(); 
                while (it.hasNext()) 
                { 
                    Map.Entry pair = (Map.Entry)it.next(); 
                    
                    ArrayList<Double> avgPrices = (ArrayList)pair.getValue(); 
                    Collections.sort(avgPrices); 
                    double median = 0.0; 
                    int size = avgPrices.size(); 
                    if(size%2 != 0) 
                        median = avgPrices.get(size/2); 
                    else 
                        median = (avgPrices.get(size/2 - 1) + avgPrices.get(size/2))/2; 
                    
                    String key = (String)pair.getKey(); 
                    String carrier = key.substring(0,2); 
                    String month = key.substring(3,key.length()); 
                    if(activeFlightsSeq.contains(carrier)) 
                    { 
                    out.write(carrier+"\t"+month+"\t"+median); 
                    out.newLine(); 
                    System.out.println(carrier+"\t"+month+"\t"+median); 
                    } 
                    it.remove();  
                } 
                 
                out.close(); 
            } 
             
         // Calculate the fast median for sequential analysis
            public void medianFastSeq(TreeMap<String, ArrayList<Double>> seqMap)throws IOException 
            { 
                FileWriter fstream = new FileWriter(outputPath+"Fast Median Sequentail.txt"); 
                BufferedWriter out = new BufferedWriter(fstream); 
                Iterator it = seqMap.entrySet().iterator(); 
                while (it.hasNext()) 
                { 
                    Map.Entry pair = (Map.Entry)it.next(); 
                    ArrayList<Double> avgPrices = (ArrayList)pair.getValue(); 
                    double fastMedian = quickSelect(avgPrices, avgPrices.size()/2); 
                    String key = (String)pair.getKey(); 
                    String carrier = key.substring(0,2); 
                    String month = key.substring(3,key.length()); 
                    if(activeFlightsSeq.contains(carrier)) 
                    { 
                    out.write(carrier+"\t"+month+"\t"+fastMedian); 
                    out.newLine(); 
                    System.out.println(pair.getKey()+"\t"+fastMedian); 
                    } 
                    it.remove();  
                } 
                out.close(); 
            } 
             
     } 
      
     // Multi Threaded program
     public class MultiThreaded  
     { 
    	// Read files in a multi threaded way
        public TreeMap readFile(String dir)throws InterruptedException 
        { 
            TreeMap<String, ArrayList<Double>> map = new TreeMap<String, ArrayList<Double>>(); 
             
            File myDirectory = new File(dir); 
             System.out.println(myDirectory.getName()); 
             File[] containingFileNames = myDirectory.listFiles(); 
             ReadThread t[]; 
             t = new ReadThread[containingFileNames.length]; 
             
             // Make threads for the number of files present in the input
            for(int i=0; i<containingFileNames.length; i++) 
            { 
                File file[] = new File[1]; 
                file[0] =  containingFileNames[i]; 
                t[i] = new ReadThread(file); 
            } 
             
            // Start the thread
            for(int i=0; i<t.length; i++) 
            { 
                t[i].start(); 
            } 
            
            // Wait for the thread
            for(int i=0; i<t.length; i++) 
            { 
                t[i].join(); 
            } 
             
            // Add all values to a map
            for(int i=0; i<t.length; i++) 
            { 
                map.putAll(t[i].map); 
            } 
             
            return map; 
        } 
         
        // Calculate mean 
        public void meanMultiThread(TreeMap<String, ArrayList<Double>> map)throws IOException 
        { 
            FileWriter fstream = new FileWriter(outputPath+"Mean Multithreded.txt"); 
            BufferedWriter out = new BufferedWriter(fstream); 
            Iterator it = map.entrySet().iterator(); 
            while (it.hasNext()) 
            { 
                Map.Entry pair = (Map.Entry)it.next(); 
                 
                ArrayList<Double> avgPrices = (ArrayList)pair.getValue(); 
                double sum = 0; 
                for(double price : avgPrices) 
                { 
                    sum+=price; 
                } 
                sum = sum/avgPrices.size(); 
                String key = (String)pair.getKey(); 
                String carrier = key.substring(0,2); 
                String month = key.substring(3,key.length()); 
                if(activeFlightsMT.contains(carrier)) 
                { 
                    out.write(carrier+"\t"+month+"\t"+sum); 
                    out.newLine(); 
                    System.out.println(pair.getKey()+"\t"+sum); 
                } 
                it.remove();  
            } 
            out.close(); 
        } 
         
        // Calculate median
        public void medianMultiThreaded(TreeMap<String, ArrayList<Double>> map)throws IOException 
        { 
            FileWriter fstream = new FileWriter(outputPath+"Median Multithreded.txt"); 
            BufferedWriter out = new BufferedWriter(fstream); 
            Iterator it = map.entrySet().iterator(); 
            while (it.hasNext()) 
            { 
                Map.Entry pair = (Map.Entry)it.next(); 
                
                ArrayList<Double> avgPrices = (ArrayList)pair.getValue(); 
                Collections.sort(avgPrices); 
                double median = 0.0; 
                int size = avgPrices.size(); 
                if(size%2 != 0) 
                    median = avgPrices.get(size/2); 
                else 
                    median = (avgPrices.get(size/2 - 1) + avgPrices.get(size/2))/2; 
                String key = (String)pair.getKey(); 
                String carrier = key.substring(0,2); 
                String month = key.substring(3,key.length()); 
                if(activeFlightsMT.contains(carrier)) 
                { 
                    out.write(carrier+"\t"+month+"\t"+median); 
                    out.newLine(); 
                    System.out.println(pair.getKey()+"\t"+median); 
                } 
                it.remove();  
            } 
            out.close(); 
        } 
        
        // Calculate fast median
        public void medianFastMultiThreaded(TreeMap<String, ArrayList<Double>> map)throws IOException 
        { 
            FileWriter fstream = new FileWriter(outputPath+"Fast Median Multithreded.txt"); 
            BufferedWriter out = new BufferedWriter(fstream); 
            Iterator it = map.entrySet().iterator(); 
            while (it.hasNext()) 
            { 
                Map.Entry pair = (Map.Entry)it.next(); 
                ArrayList<Double> avgPrices = (ArrayList)pair.getValue(); 
                double fastMedian = quickSelect(avgPrices, avgPrices.size()/2); 
                String key = (String)pair.getKey(); 
                String carrier = key.substring(0,2); 
                String month = key.substring(3,key.length()); 
                if(activeFlightsMT.contains(carrier)) 
                { 
                    out.write(carrier+"\t"+month+"\t"+fastMedian); 
                    out.newLine(); 
                System.out.println(pair.getKey()+"\t"+fastMedian); 
                } 
                it.remove();  
            } 
             
            out.close(); 
        } 
         
     } 
      
     // Thread class
     class ReadThread extends Thread 
     { 
         public File[] containingFileNames; 
         TreeMap<String, ArrayList<Double>> map = new TreeMap<String, ArrayList<Double>>(); 
         ReadThread(File[] file) 
         { 
             containingFileNames = file; 
         } 
          
         public void run() 
            {     
                    for(File fileName : containingFileNames) 
                    { 
                        GZIPInputStream gzip = null; 
                        try  
                        { 
                            gzip = new GZIPInputStream(new FileInputStream(fileName)); 
                        } 
                        catch (FileNotFoundException e)  
                        { 
                            e.printStackTrace(); 
                        }  
                        catch (IOException e) 
                        { 
                            e.printStackTrace(); 
                        } 
                        BufferedReader br = new BufferedReader(new InputStreamReader(gzip)); 
                        String content; 
                        try  
                        { 
                                while ((content = br.readLine()) != null) 
                                { 
                                    String line = content.toString(); 
                                    line = line.replaceAll("\"", ""); 
                                    String formatedData = line.replaceAll(", ", ":"); 
                                    String[] data = formatedData.split(","); 
                                    boolean sanityTest = sanityTest(data);  
                                    if(sanityTest) 
                                    { 
                                        String carrier = data[8]; 
                                        int year = Integer.parseInt(data[0]); 
                                        int month = Integer.parseInt(data[2]); 
                                        double price = Double.parseDouble(data[109]); 
                                        if(year == 2015) 
                                        { 
                                            synchronized(HW1.activeFlightsMT) 
                                            { 
                                                activeFlightsMT.add(carrier);     
                                            } 
                                        } 
                                         
                                        String key = carrier+"_"+month; 
                                        if(map.containsKey(key)) 
                                        { 
                                            ArrayList<Double> list = map.get(key); 
                                            list.add(price); 
                                            map.put(key, list); 
                                        } 
                                        else 
                                        { 
                                            ArrayList<Double> list = new ArrayList(); 
                                            list.add(price); 
                                            map.put(key,list); 
                                        } 
                                    } 
                                  
                                } 
                        }  
                        catch (IOException e)  
                        { 
                            e.printStackTrace(); 
                        } 
                         
                    } 
            } 
          
    
    // Sanity Test for multi threaded program
    public boolean sanityTest(String data[]) 
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
                             
            if (cancelled != 1) 
            { 
                if (((timeToMinute(Integer.valueOf(data[41])) - timeToMinute(Integer.valueOf(data[30])) - Integer.valueOf(data[51]) - timeZone)/60)%24 == 0) { 
                    if (Float.valueOf(data[42]) > 0) 
                    { 
                        if (Float.valueOf(data[42]).equals(Float.valueOf(data[43]))) 
                            validData = true; 
                        else 
                            return false; 
                     } 
                                     
                     if (Float.valueOf(data[42]) < 0) 
                     { 
                         if (Float.valueOf(data[43]) == 0) 
                             validData = true; 
                         else 
                             return false; 
                     } 
                                     
                     if (Float.valueOf(data[43]) >= 15) 
                     { 
                         if (Float.valueOf(data[44]) == 1) 
                             validData = true; 
                         else 
                             return false; 
                     } 
                                 
                    }  
                else 
                   return false; 
                            
            } 
                         
            } else  
            { 
                            return false; 
                         
            } 
                } 
                catch(Exception e) 
                { 
                    return false; 
                } 
                 
                return validData; 
            } 
     
           
     } 
}
