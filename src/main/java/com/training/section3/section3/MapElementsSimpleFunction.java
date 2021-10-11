package com.training.section3.section3;

import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

class User extends SimpleFunction<String, String>{
	
	@Override
	public String apply(String input) {
		// TODO Auto-generated method stub
		
		String arr[] = input.split(",");
		String SId= arr[0];
		String UId= arr[1];
		String Uname= arr[2];		
		String VId= arr[3];	
		String duration= arr[4];				
		String startTime= arr[5];	
		String sex= arr[6];	
	
		String ouput="";
		if(sex.equals("1")) {
			ouput=SId+","+UId+","+","+Uname+","+VId+","+duration+","+startTime+","+"M";
		}else if(sex.equals("2")) {
			ouput=SId+","+UId+","+","+Uname+","+VId+","+duration+","+startTime+","+"F";
		}
		else {
			ouput=input;
		}
				
		return ouput;
	}
	
}

public class MapElementsSimpleFunction {

	
	public static void main(String[] args) {
		Pipeline p = Pipeline.create();
		
		PCollection<String> pUserList= p.apply(TextIO.read().from("/Users/sandy/BEAM/section3/src/main/resources/section3/user.csv"));

		//Using Simple Function

		PCollection<String> pOutput= pUserList.apply(MapElements.via(new User()));
		
		pOutput.apply(TextIO.write().to("/Users/sandy/BEAM/section3/src/main/resources/section3/user_output.csv").withNumShards(1).withSuffix(".csv"));
		
		p.run();
	}
}

