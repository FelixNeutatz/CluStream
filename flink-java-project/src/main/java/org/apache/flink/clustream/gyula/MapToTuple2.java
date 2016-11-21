package org.apache.flink.clustream.gyula;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.math.DenseVector;
import org.apache.flink.ml.math.Vector;

/**
 * Created by felix on 08.11.16.
 */
public class MapToTuple2 implements MapFunction<String, Tuple2<Long,Integer>> {
	@Override
	public Tuple2<Long,Integer> map(String value) throws Exception {
		String [] splits = value.split(",");
		return new Tuple2<>(Long.parseLong(splits[0]), Integer.parseInt(splits[1]));
	}
}
