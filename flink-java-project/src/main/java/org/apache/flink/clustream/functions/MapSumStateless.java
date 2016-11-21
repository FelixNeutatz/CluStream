package org.apache.flink.clustream.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

public class MapSumStateless extends RichMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

	private int localcount;

	public void open(Configuration cfg) {
		localcount = 0;
	}
	
	@Override
	public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {
		localcount++;
		System.out.println("local count: " + localcount);
		
		return value;
	}
}
