import java.io.*;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.CSVFormat;

public class HW0 {	
	//Path of the data file
	private static final String FileLocation = "/Users/Neha/Downloads/323.csv";
	public static void main (String[] args){
		/* Object of CalculateAndPrintValues. This class is for calculating values of K and F as well as
		 * Carrier and mean price of tickets. This class will also print these values.
		*/
		CalculateAndPrintValues calc = new CalculateAndPrintValues();
		/* Invoke the method in CalculateAndPrintValues which is responsible for calculating 
		 * and printing values for K, F, C, P
		*/
		calc.calculateValues(getRecords(CalculateAndPrintValues.getColumns()));				
	}	
	public static Iterable<CSVRecord> getRecords(String[] columns) {
		Iterable<CSVRecord> records = null;
		try {
			// Read file from the following location
			Reader reader = new FileReader(FileLocation);
			records = CSVFormat.EXCEL.withHeader(columns).withSkipHeaderRecord(true).parse(reader);
		}
		catch(FileNotFoundException f) {
			new Error(f);
		}
		catch(IOException i) {
			new Error(i);
		}
		return records;
	} 
}
