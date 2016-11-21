package org.apache.flink.clustream.functions;

import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * Created by felix on 09.11.16.
 */
public class SumReduceFunction implements ReduceFunction<Integer> {

	@Override
	public Integer reduce(Integer value1,
						  Integer value2) throws
	Exception {
	return new Integer(value1 + value2);
	}
	
}
