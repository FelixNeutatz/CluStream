package org.apache.flink.clustream.gyula;

/**
 * Created by felix on 21.11.16.
 */
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

public class MySource implements ParallelSourceFunction<Tuple2<Long, Integer>> {
	private static final long serialVersionUID = 1L;

	private int numKeys;

	private volatile boolean running = true;

	public MySource(int numKeys) {
		this.numKeys = numKeys;
	}

	@Override
	public void run(SourceContext<Tuple2<Long, Integer>> out) throws Exception {
		while (running) {
			out.collect(new Tuple2<Long, Integer>(0L, 10));
			out.collect(new Tuple2<Long, Integer>(1L, 20));
			out.collect(new Tuple2<Long, Integer>(2L, 10));
			out.collect(new Tuple2<Long, Integer>(3L, 20));
			
			Thread.sleep(1000);
			
		}
	}

	@Override
	public void cancel() {
		this.running = false;
	}
}
