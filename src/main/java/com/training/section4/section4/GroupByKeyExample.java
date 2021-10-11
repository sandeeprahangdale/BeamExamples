package com.training.section4.section4;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

// STEP 2
class StringToKV extends DoFn<String,KV<String, Integer>> {

	@ProcessElement
	public void processElement(ProcessContext c) {
		
		String input=c.element();
		String arr[] = input.split(",");
		c.output(KV.of(arr[0], Integer.valueOf(arr[3])));

	}	
}

//STEP 4

class KVToString extends DoFn<KV<String, Iterable<Integer>>,String> {

	@ProcessElement
	public void processElement(ProcessContext c) {

		String strKey=c.element().getKey();
		Iterable<Integer> vals=c.element().getValue();
		
		Integer sum=0;
		for (Integer integer : vals) {
			sum=sum+integer;
		}
		
		c.output(strKey+","+sum.toString());
	}	
}


public class GroupByKeyExample {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Pipeline p = Pipeline.create();

		// Step 1
		PCollection<String> pCustOrderList = p.apply(TextIO.read().from("/Users/sandy/BEAM/section3/src/main/resources/section4/GroupByKey_data.csv"));

		// Step 2 : Convert String to KV
		
		PCollection<KV<String,Integer>> kvOrder = pCustOrderList.apply(ParDo.of(new StringToKV()));

		 
		// Step 3 : Apply groupbyKey and build KV<String, Iterable<Integer>
		
		PCollection<KV<String,Iterable<Integer>>> kvOrder2 =  kvOrder.apply(GroupByKey.<String, Integer>create());

	
		// Step 4 : Convert KV<String, Iterable<Integer> to String and write
		
		PCollection<String> output = kvOrder2.apply(ParDo.of(new KVToString()));
		
		output.apply(TextIO.write().to("/Users/sandy/BEAM/section3/src/main/resources/section4/group_by_output.csv").withHeader("Id,Amount").withNumShards(1).withSuffix(".csv"));
		
		p.run();
	}

}

