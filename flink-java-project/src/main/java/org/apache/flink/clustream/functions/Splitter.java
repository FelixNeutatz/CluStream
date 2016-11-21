package org.apache.flink.clustream.functions;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.math.Vector;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by felix on 08.11.16.
 */
public class Splitter implements OutputSelector<Tuple3<Long,Long,Vector>> {
	private long split_limit;
	
	public Splitter(long split_limit) {
		this.split_limit = split_limit;
	}
	
	@Override
	public Iterable<String> select(Tuple3<Long,Long,Vector> value) {
		List<String> output = new ArrayList<String>();
		if (value.f1 < split_limit) {
			output.add("Kmeans");
		}
		else {
			output.add("CluStream");
		}
		return output;
	}
}
