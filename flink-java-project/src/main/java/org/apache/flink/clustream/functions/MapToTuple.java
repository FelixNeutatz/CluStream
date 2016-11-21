package org.apache.flink.clustream.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.math.Vector;

/**
 * Created by felix on 08.11.16.
 */
public class MapToTuple implements MapFunction<Vector, Tuple2<Long, Vector>> {
	@Override
	public Tuple2<Long, Vector> map(Vector value) throws Exception {
		return new Tuple2<Long, Vector>(System.nanoTime() % 1000,value);
	}
}

