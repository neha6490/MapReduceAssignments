public class Columns {	
	private static final String COLUMNS[] = {
			"YEAR","QUARTER","MONTH","DAY_OF_MONTH","DAY_OF_WEEK","FL_DATE","UNIQUE_CARRIER",
			"AIRLINE_ID","CARRIER","TAIL_NUM","FL_NUM","ORIGIN_AIRPORT_ID","ORIGIN_AIRPORT_SEQ_ID",
			"ORIGIN_CITY_MARKET_ID","ORIGIN","ORIGIN_CITY_NAME","ORIGIN_STATE_ABR","ORIGIN_STATE_FIPS",
			"ORIGIN_STATE_NM","ORIGIN_WAC","DEST_AIRPORT_ID","DEST_AIRPORT_SEQ_ID","DEST_CITY_MARKET_ID",
			"DEST","DEST_CITY_NAME","DEST_STATE_ABR","DEST_STATE_FIPS","DEST_STATE_NM","DEST_WAC","CRS_DEP_TIME",
			"DEP_TIME","DEP_DELAY","DEP_DELAY_NEW","DEP_DEL15","DEP_DELAY_GROUP","DEP_TIME_BLK","TAXI_OUT",
			"WHEELS_OFF","WHEELS_ON","TAXI_IN","CRS_ARR_TIME","ARR_TIME","ARR_DELAY","ARR_DELAY_NEW","ARR_DEL15",
			"ARR_DELAY_GROUP","ARR_TIME_BLK","CANCELLED","CANCELLATION_CODE","DIVERTED","CRS_ELAPSED_TIME",
			"ACTUAL_ELAPSED_TIME","AIR_TIME","FLIGHTS","DISTANCE","DISTANCE_GROUP","CARRIER_DELAY","WEATHER_DELAY",
			"NAS_DELAY","SECURITY_DELAY","LATE_AIRCRAFT_DELAY","FIRST_DEP_TIME","TOTAL_ADD_GTIME",
			"LONGEST_ADD_GTIME","DIV_AIRPORT_LANDINGS","DIV_REACHED_DEST","DIV_ACTUAL_ELAPSED_TIME",
			"DIV_ARR_DELAY","DIV_DISTANCE","DIV1_AIRPORT","DIV1_AIRPORT_ID","DIV1_AIRPORT_SEQ_ID",
			"DIV1_WHEELS_ON","DIV1_TOTAL_GTIME","DIV1_LONGEST_GTIME","DIV1_WHEELS_OFF","DIV1_TAIL_NUM",
			"DIV2_AIRPORT","DIV2_AIRPORT_ID","DIV2_AIRPORT_SEQ_ID","DIV2_WHEELS_ON","DIV2_TOTAL_GTIME",
			"DIV2_LONGEST_GTIME","DIV2_WHEELS_OFF","DIV2_TAIL_NUM","DIV3_AIRPORT","DIV3_AIRPORT_ID",
			"DIV3_AIRPORT_SEQ_ID","DIV3_WHEELS_ON","DIV3_TOTAL_GTIME","DIV3_LONGEST_GTIME","DIV3_WHEELS_OFF",
			"DIV3_TAIL_NUM","DIV4_AIRPORT","DIV4_AIRPORT_ID","DIV4_AIRPORT_SEQ_ID",
			"DIV4_WHEELS_ON","DIV4_TOTAL_GTIME","DIV4_LONGEST_GTIME","DIV4_WHEELS_OFF","DIV4_TAIL_NUM",
			"DIV5_AIRPORT","DIV5_AIRPORT_ID","DIV5_AIRPORT_SEQ_ID","DIV5_WHEELS_ON","DIV5_TOTAL_GTIME",
			"DIV5_LONGEST_GTIME","DIV5_WHEELS_OFF","DIV5_TAIL_NUM","AVG_TICKET_PRICE"
			};	
	public static String[] getColumns() {
		return COLUMNS;
	}
}
