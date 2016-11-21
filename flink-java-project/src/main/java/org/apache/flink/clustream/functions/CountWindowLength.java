package org.apache.flink.clustream.functions;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.math.Vector;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * Created by felix on 08.11.16.
 */
public class CountWindowLength implements WindowFunction<Tuple3<Long, Long,Vector>, Tuple1<Integer>, Tuple, TimeWindow> {
	public void apply (Tuple tuple, TimeWindow window, Iterable<Tuple3<Long, Long,Vector>> values, Collector<Tuple1<Integer>> out) throws Exception {
		int sum = 0;
		Iterator iterator = values.iterator();
		while (iterator.hasNext () ) {
			Tuple3<Long, Long,Vector> t = (Tuple3<Long, Long,Vector>)iterator.next();
		sum += 1;
		}
		out.collect (new Tuple1<Integer>(new Integer(sum)));
	}
}
