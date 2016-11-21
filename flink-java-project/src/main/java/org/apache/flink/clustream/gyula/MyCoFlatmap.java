package org.apache.flink.clustream.gyula;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Created by felix on 15.11.16.
 */
public class MyCoFlatmap extends RichCoFlatMapFunction<Tuple2<Long, Integer>, Average, Average> {

	private Average localAverage;
	
	private Average delta;
	
	private int recordCount;
	private final int synchronizationStepSize;

	public MyCoFlatmap(int synchronizationStepSize) {
		this.synchronizationStepSize = synchronizationStepSize;
	}
	
	@Override
	public void open(Configuration parameters) throws Exception {
		localAverage = new Average();
		localAverage.N = 0;
		localAverage.sum = 0;
		recordCount = 0;

		localAverage.subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

		delta = new Average();
		delta.N = 0;
		delta.sum = 0;
		delta.subtaskIndex = localAverage.subtaskIndex;
	}
	
	
	@Override
	public void flatMap1(Tuple2<Long,Integer> value, Collector<Average> out) throws Exception {
		//local average update
		localAverage.sum += value.f1;
		localAverage.N++;
		recordCount++;

		delta.sum += value.f1;
		delta.N++;

		if (recordCount == synchronizationStepSize) {
			out.collect(delta);

			recordCount = 0;
			
			//clean delta
			delta.sum = 0;
			delta.N = 0;
		}
		}

	@Override
	public void flatMap2(Average value, Collector<Average> out) throws Exception {
		if (value.subtaskIndex != localAverage.subtaskIndex) {
			localAverage.sum += value.sum;
			localAverage.N += value.N;
		}

		System.out.println("receive: " + localAverage);
	}
}
