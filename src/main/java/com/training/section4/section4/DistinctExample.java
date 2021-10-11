package com.training.section4.section4;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;


public class DistinctExample {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Pipeline p = Pipeline.create();

		PCollection<String> pCustList = p.apply(TextIO.read().from("/Users/sandy/BEAM/section3/src/main/resources/section4/Distinct.csv"));

		PCollection<String> uniqueCust=pCustList.apply(Distinct.<String>create());
				
		uniqueCust.apply(TextIO.write().to("/Users/sandy/BEAM/section3/src/main/resources/section4/distinct_out.csv").withNumShards(1).withSuffix(".csv"));
		
		p.run();
	}

}

