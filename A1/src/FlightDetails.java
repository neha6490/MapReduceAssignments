import java.util.ArrayList;
public class FlightDetails {	
	int frequency;
	String carrier;
	double totalPrice;
	double completeAvgPrice;
	ArrayList<Double> listOfPrice = new ArrayList<Double>();
	FlightDetails(int f, String c, double ap)
	{
		frequency = f;
		carrier = c;
		totalPrice = ap;
		listOfPrice.add(ap);
	}
	public String getCarrier() {
		return carrier;
	}
	public void setCarrier(String carrier) {
		this.carrier = carrier;
	}
	public int getFrequency() {
		return frequency;
	}
	public void setFrequency(int frequency) {
		this.frequency = frequency;
	}	
	public double getTotalPrice() {
		return totalPrice;
	}
	public void setTotalPrice(double totalPrice) {
		this.totalPrice = totalPrice;
	}
	public void setList(double avgPrice){
		listOfPrice.add(avgPrice);
	}
	public double calculateAvgPrice()
	{
		return totalPrice/frequency;
	}
}
