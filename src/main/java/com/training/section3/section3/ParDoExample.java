package com.training.section3.section3;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

class CustFilter extends DoFn<String, String> {

	@ProcessElement
	public void processElement(ProcessContext c) {
		String line = c.element();
		String arr[] = line.split(",");
		
		if(arr[3].equals("Los Angeles")) {
			c.output(line);
		}

	}
}

class MyFilter implements SerializableFunction<String,Boolean>{

	@Override
	public Boolean apply(String input) {
		
		return input.contains("Los Angeles");
	}
	
}

public class ParDoExample {

	public static void main(String[] args) {

		Pipeline p = Pipeline.create();

		PCollection<String> pCustList = p.apply(TextIO.read().from("/Users/sandy/BEAM/section3/src/main/resources/section3/customer_pardo.csv"));

		// Using ParDO

		//PCollection<String> pOutput = pCustList.apply(ParDo.of(new CustFilter()));
		
		//pOutput.apply(TextIO.write().to("/Users/sandy/BEAM/section3/src/main/resources/customer_pardo_output.csv").withHeader("Id,Name,Last Name,City").withNumShards(1).withSuffix(".csv"));
		
		PCollection<String> pOutput = pCustList.apply(Filter.by(new MyFilter()));
		
		pOutput.apply(TextIO.write().to("/Users/sandy/BEAM/section3/src/main/resources/section3/customer_filter_pardo_output.csv").withHeader("Id,Name,Last Name,City").withNumShards(1).withSuffix(".csv"));
		
		p.run();

	}

}

