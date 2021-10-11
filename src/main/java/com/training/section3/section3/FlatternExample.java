package com.training.section3.section3;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;



public class FlatternExample {

	public static void main(String[] args) {

		Pipeline p = Pipeline.create();

		PCollection<String> pCustList1 = p.apply(TextIO.read().from("/Users/sandy/BEAM/section3/src/main/resources/section3/customer_1.csv"));

		PCollection<String> pCustList2 = p.apply(TextIO.read().from("/Users/sandy/BEAM/section3/src/main/resources/section3/customer_2.csv"));
		
		PCollection<String> pCustList3 = p.apply(TextIO.read().from("/Users/sandy/BEAM/section3/src/main/resources/section3/customer_3.csv"));		
		
		PCollectionList<String> list=PCollectionList.of(pCustList1).and(pCustList2).and(pCustList3);
		
		PCollection<String> merged=list.apply(Flatten.pCollections());
		
		merged.apply(TextIO.write().to("/Users/sandy/BEAM/section3/src/main/resources/section3/customer_flatteren_output.csv").withHeader("Id,Name,Last Name,City").withNumShards(1).withSuffix(".csv"));
		
		p.run();

	}

}

