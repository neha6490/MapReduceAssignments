import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import static java.time.temporal.ChronoUnit.MINUTES;
import org.apache.commons.csv.CSVRecord;

public class SanityTest {
	//conditions check is the main method for all the sanity checks. 
	public boolean conditionsCheck(CSVRecord record) {
		return (ArrDepTime(record) || 
				timeZone(record) || 
				conditionForZero(record) || 
				conditionForEmpty(record) ||
				cancelledFlights(record));
	}
	// check if CRSArrTime and CRSDepTime is zero
	private boolean ArrDepTime(CSVRecord record) {
		return (Integer.parseInt(record.get("CRS_ARR_TIME")) == 0 || 
				Integer.parseInt(record.get("CRS_DEP_TIME")) == 0);
	}	
	//timeZone = CRSArrTime - CRSDepTime - CRSElapsedTime;
	//check if timeZone % 60 is 0
	private boolean timeZone(CSVRecord record) {
		int diff = (int) (MINUTES.between(changeToLocalTime(record.get("CRS_DEP_TIME")), changeToLocalTime(record.get("CRS_ARR_TIME"))));
		int timeZone = diff-Integer.parseInt(record.get("CRS_ELAPSED_TIME"));
		return (timeZone%60 != 0); 
	}
	//check if AirportID,  AirportSeqID, CityMarketID, StateFips, Wac is larger than 0
	private boolean conditionForZero(CSVRecord record) {
		return (Integer.parseInt(record.get("ORIGIN_AIRPORT_ID")) <= 0 || 
				Integer.parseInt(record.get("DEST_AIRPORT_ID")) <= 0 ||
				Integer.parseInt(record.get("ORIGIN_AIRPORT_SEQ_ID")) <= 0 || 
				Integer.parseInt(record.get("DEST_AIRPORT_SEQ_ID")) <= 0 || 
				Integer.parseInt(record.get("ORIGIN_CITY_MARKET_ID")) <= 0 ||
				Integer.parseInt(record.get("DEST_CITY_MARKET_ID")) <= 0 || 
				Integer.parseInt(record.get("ORIGIN_STATE_FIPS")) <= 0 ||
				Integer.parseInt(record.get("DEST_STATE_FIPS")) <= 0 ||
				Integer.parseInt(record.get("ORIGIN_WAC")) <= 0 || 
				Integer.parseInt(record.get("DEST_WAC")) <= 0);
	}
	//check if Origin, Destination,  CityName, State, StateName is empty
	private boolean conditionForEmpty(CSVRecord record){
		return (record.get("ORIGIN") == null||
				record.get("DEST") == null ||
				record.get("ORIGIN_CITY_NAME") == null || 
				record.get("DEST_CITY_NAME") == null || 
				record.get("ORIGIN_STATE_NM") == null || 
				record.get("ORIGIN_STATE_ABR") == null);
	}
	// check if flights are cancelled. If not cancelled check further for more conditions.
	private boolean cancelledFlights(CSVRecord record) {
		if(Integer.parseInt(record.get("CANCELLED")) == 1)
			return false;	
		return (timeDiff(record) || arrDelay(record));
	} 
	// helper function for timeDiff method for formatting the time
	private LocalTime changeToLocalTime(String time) {
		return LocalTime.parse(time,DateTimeFormatter.ofPattern("HHmm"));
	}
	
	//helper function for timeDiff method for calculating the time difference in minutes
	private int calcMinutes(LocalTime arrTime, LocalTime depTime) {
		int hour;
		if(arrTime.getHour() > depTime.getHour()) {
			hour = arrTime.getHour() - depTime.getHour();
		}
		else if(arrTime.getHour() == depTime.getHour()) {
			if(arrTime.getMinute() > depTime.getMinute())
				hour = 0;
			else
				hour = 24;
		}
		else {
			hour = (24 - depTime.getHour()) + arrTime.getHour();
		}
		return ((hour * 60) + (arrTime.getMinute() - depTime.getMinute()));
	}
	//check if ArrTime -  DepTime - ActualElapsedTime - timeZone is zero
	private boolean timeDiff(CSVRecord record) {
		int timeZone = calcMinutes(changeToLocalTime(record.get("CRS_ARR_TIME")), changeToLocalTime(record.get("CRS_DEP_TIME"))) - 
				Integer.parseInt(record.get("CRS_ELAPSED_TIME"));
		int arr_dep_elapsedDiff = calcMinutes(changeToLocalTime(record.get("ARR_TIME")), changeToLocalTime(record.get("DEP_TIME"))) - 
				Integer.parseInt(record.get("ACTUAL_ELAPSED_TIME"));
		int diff = arr_dep_elapsedDiff - timeZone;
		return (diff != 0);
	}
	/* check if ArrDelay > 0 then ArrDelay should equal to ArrDelayMinutes,,
	if ArrDelay < 0 then ArrDelayMinutes should be zero,
	if ArrDelayMinutes >= 15 then ArrDel15 should be false*/
	private boolean arrDelay(CSVRecord record) {
		if(Double.parseDouble(record.get("ARR_DELAY")) > 0) {
			if(Double.parseDouble(record.get("ARR_DELAY")) == Double.parseDouble(record.get("ARR_DELAY_NEW"))) {
				if(Double.parseDouble(record.get("ARR_DELAY_NEW")) >= 15) {
					if(Double.parseDouble(record.get("ARR_DEL15")) == 1) {
						return false;
					}
				}
				else {
					return false;
				}
			}				
		}
		else {
			if(Double.parseDouble(record.get("ARR_DELAY_NEW")) == 0) {
				return false;
			}
		}
		return true;
	}
}
