import java.io.*;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.ArrayList;
import java.util.Collections;

public class cli {
    public static void main(String [] args) throws IOException {
	//author: Neha Patwardhan, Joy Machado    	
    	String fileInput = args[0];
    	ArrayList<Integer> max = new ArrayList<Integer>();  
	List<String> fileIn = new ArrayList<String>();
	File[] files = new File(fileInput).listFiles();	
	for(int i = 0; i<files.length; i++)
	{
		fileIn.add(fileInput +"/"+ files[i].getName()+"/controller.gz");				
	}		
    	for(String fileRead : fileIn)
	{
    	BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(fileRead))));   		     		String content;
    	String[] splits = null;
    	ArrayList<String> states = new ArrayList<String>();
	//author: Rohan Joshi     	
	while ((content = br.readLine()) != null){
    		splits = content.split(" ");
    	}   		
    	int size = splits.length;
    	String timeTaken = splits[size - 2];
    	int time = Integer.parseInt(timeTaken);
    	max.add(time);
    	}
	//author: Neha Patwardhan, Joy Machado
    	Collections.sort(max);
    	int endTime = max.get(max.size() - 1);	
	File file = new File("/home/neha/test/outputs" + "_" +args[1] +".csv");
	file.createNewFile();
	FileWriter writer = new FileWriter(file);
	writer.write("emr"+ "," + args[1] + "," + endTime);
	writer.write("/n");
        writer.flush();
        writer.close();		
    }
}
