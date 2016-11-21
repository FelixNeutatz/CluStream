package org.apache.flink.clustream.functions;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * Created by felix on 12.11.16.
 */
public class SumWindowFunction implements WindowFunction<Tuple2<Integer,Integer>, Tuple1<Integer>, Tuple, GlobalWindow> {
	public void apply (Tuple tuple, GlobalWindow window, Iterable<Tuple2<Integer,Integer>> values, Collector<Tuple1<Integer>> out) throws Exception {
		int sum = 0;
		Iterator iterator = values.iterator();
		while (iterator.hasNext () ) {
			Tuple2<Integer,Integer> t = (Tuple2<Integer,Integer>)iterator.next();
			sum += 1;
		}
		out.collect (new Tuple1<Integer>(new Integer(sum)));
	}
}
