package org.apache.flink.clustream.functions;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Created by felix on 09.11.16.
 */
public class SumReduceFunction2 implements ReduceFunction<Tuple2<Integer, Integer>> {

	@Override
	public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1,
										   Tuple2<Integer, Integer> value2) throws
	Exception {
	return new Tuple2<Integer, Integer>(value1.f0, new Integer(value1.f1 + value2.f1));
	}
	
}
