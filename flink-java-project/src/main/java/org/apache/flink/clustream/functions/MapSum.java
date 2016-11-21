package org.apache.flink.clustream.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

public class MapSum extends RichMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

	private transient ReducingState<Integer> state;
	
	private int localcount;

	public void open(Configuration cfg) {
		state = getRuntimeContext().getReducingState(
			new ReducingStateDescriptor<Integer>("sum", new SumReduceFunction(), Integer.class));
		
		localcount = 0;
	}
	
	@Override
	public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {
		state.add(value.f1);
		
		localcount++;
		System.out.println("state: " + state.get() + "local count: " + localcount);
		
		return value;
	}
}
