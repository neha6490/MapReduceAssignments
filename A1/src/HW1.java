import java.io.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.CSVFormat;
public class HW1 {
	/*declaring a HashSet for holding the carrier list of those airlines which are active
	in January 2015 */
	 public static HashSet carriers = new HashSet();
	 
	 //declaring Map for holding the list of all the carriers and their avg values
	 public static Map<String,FlightDetails> map = new HashMap<String, FlightDetails>();
	 
	 public static void main (String[] args) throws InterruptedException{
		// Creating thread pool of 5 threads 
		WorkThread threads[]= new WorkThread[5];
		
		//Initialization. Input parameter is mandatory
		String path = "please mention some path";
		// Flag initialization.
		String flag = "no";
		
		/* If user has provided only one input, then that input has to be the path to the directory
		 * from where we want to read the files.
		 */
		if(args.length == 1){
			path = args[0];
		} 
		//else read both the parameters.
		else {
			flag = args[0];
			path = args[1];
		}

		File mydir = new File(path);
		
		//get all the files from the directory
		File[] listOfFiles = mydir.listFiles();
		
		//Sequential access if flag is set to "no". That means no parallel access
		if(flag.equals("no")){
			//System.out.println("Running in sequential mode. Takes time upto 180 seconds to print the output");
			WorkThread t0 = new WorkThread(listOfFiles);
			t0.start();
			t0.join();			 
			 System.out.println("K:" + t0.K );
			 System.out.println("F:" + t0.F);
			    
		}
		//Parallel access
		else{	
			//System.out.println("Running in parallel mode....Reading Files....");
			//create 5 threads.
			for(int i=0,j=0;i<25&&j<5;i+=5,j++){
				//divide list of files so that every thread will run 5 files.
				File[] filesPart = new File[5];
				System.arraycopy(listOfFiles, i, filesPart, 0, 5);
				threads[j] = new WorkThread(filesPart);
			}
			
			//start all the threads
			for(int i=0;i<5;i++){
				threads[i].start();
			}
			
			for(int i=0;i<5;i++){
				threads[i].join();
			}
						
			long count = 0;
			long k = 0;
			long f = 0;
			//get total K and F from all the threads
			for(int i=0;i<5;i++){
				k+= threads[i].K;
				f+= threads[i].F;
			}		
			System.out.println("K:" + k );
			System.out.println("F:" + f);
		}
		
		// Create TreeMap to get sorted list
	    TreeMap<String, String> sortedMap = new TreeMap<String, String>();
	    
	    //iterate through the list which contains all the carriers and their ticket prices
		Iterator it = map.entrySet().iterator();
		double median = 0;
	    while (it.hasNext())
	    {	
	        Map.Entry pair = (Map.Entry)it.next();
	        FlightDetails flight = (FlightDetails)pair.getValue();
	        //get list of all the prices for a carrier
	        List<Double> listOfPrices = flight.listOfPrice;
	        
	        listOfPrices.removeAll(Collections.singleton(null));
	        //sort the list to find the medians
	        Collections.sort(listOfPrices);
	        
	        // find medians for all the carriers
	        if (listOfPrices.size() % 2 == 0)
	          median = (listOfPrices.get(listOfPrices.size()/2) + listOfPrices.get(listOfPrices.size()/2 - 1))/2;
	        else
	          median = listOfPrices.get(listOfPrices.size()/2);
	          sortedMap.put(flight.calculateAvgPrice()+"//"+median, flight.getCarrier());
	          it.remove(); 
	    }
	    
	    //Print values for only those carriers which were active in January 2015
	    for (Map.Entry<String,String> entry : sortedMap.entrySet()) {
	    	if(carriers.contains(entry.getValue())){
	        System.out.println(entry.getValue() + " " + entry.getKey().replace("//", " "));
	    	}
	    }
	 }
}

class WorkThread extends Thread {
	//Initializing K and F
	public int K = 0;
	public int F = 0;
    public File[] list1;
    
    //Let every thread know what set of files it supposed to read
    public WorkThread(File[] files) {
		list1 = files;
	}
    public void run() {
		try {
			Iterable<CSVRecord> records = null;
			 for (int i = 0; i < list1.length; i++) {
				 //Object for SantityTest check.
					SanityTest test = new SanityTest();	
					
					// Read files
			        FileInputStream inputStream = new FileInputStream(list1[i]);
			        GZIPInputStream gInputStream = new GZIPInputStream(inputStream);
			        InputStreamReader inputStreamReader = new InputStreamReader(gInputStream);
			        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
					records = CSVFormat.EXCEL.withHeader(Columns.getColumns()).withSkipHeaderRecord(true).parse(bufferedReader);	
					
					//increment K if record is not consistent
					for (CSVRecord record : records) {
						if(!record.isConsistent()) {
							K++;
							continue;
						}
						
						// If the record does not pass the sanity check, then increment K else increment F
						if(test.conditionsCheck(record)) {
							K++;
							continue;
						}
						
						//Synchronized access to static map to avoid inconsistent access to the list
							synchronized(HW1.map){
								//maintain a list and put carriers that were active in January 2015 
								if(record.get("YEAR").equals("2015") && record.get("MONTH").equals("1")){
									HW1.carriers.add(record.get("CARRIER"));
								}
								//if carrier is present in the list, update total ticket price
								if(HW1.map.containsKey(record.get("CARRIER")))
								{
									FlightDetails f = (FlightDetails) HW1.map.get(record.get("CARRIER"));
									double price = f.getTotalPrice();
									f.setTotalPrice(price + Double.parseDouble(record.get("AVG_TICKET_PRICE")));
									f.setFrequency(f.getFrequency() + 1);
									f.setList(Double.parseDouble(record.get("AVG_TICKET_PRICE")));
								}
								else
								{	//create a FlightDetails object and add the ticket price to it
									FlightDetails f = new FlightDetails(1, (record.get("CARRIER")), Double.parseDouble(record.get("AVG_TICKET_PRICE")));
									HW1.map.put(record.get("CARRIER"), f);
								}
							}
							F++;	
						}										
			}			 
		}			 			 
		catch(FileNotFoundException f) {
			new Error(f);
		}
		catch(IOException i) {
			new Error(i);
		}	
		}				
    }
