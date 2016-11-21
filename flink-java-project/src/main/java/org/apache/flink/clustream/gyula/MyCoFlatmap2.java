package org.apache.flink.clustream.gyula;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Created by felix on 15.11.16.
 */
public class MyCoFlatmap2 extends RichCoFlatMapFunction<Tuple2<Long, Integer>, Tuple3<Long,Integer,Integer>, Tuple3<Long,Integer,Integer>> {

	private transient ValueState<Tuple2<Integer,Integer>> localAverage;
	
	private int deltaSum;
	private int deltaN;
	
	
	private int recordCount;
	private int synchronizationStepSize;
	private int parallelism;
	private int subtaskIndex;

	public MyCoFlatmap2() {
	}
	
	public MyCoFlatmap2(int synchronizationStepSize) {
		this.synchronizationStepSize = synchronizationStepSize;
	}
	
	@Override
	public void open(Configuration parameters) throws Exception {
		ValueStateDescriptor descriptor =
			new ValueStateDescriptor<>(
				"localAverage", // the state name
				TypeInformation.of(new TypeHint<Tuple2<Integer,Integer>>() {}), // type information
				Tuple2.of(0, 0)); // default value of the state, if nothing was set
		localAverage = getRuntimeContext().getState(descriptor);
		
		recordCount = 0;

		deltaSum = 0;
		deltaN = 0;
		
		subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
		parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
	}
	
	
	@Override
	public void flatMap1(Tuple2<Long,Integer> value, Collector<Tuple3<Long,Integer,Integer>> out) throws Exception {
		//local average update
		Tuple2<Integer, Integer> avg = localAverage.value();
		localAverage.update(new Tuple2<Integer, Integer>(avg.f0 + value.f1, avg.f1 + 1));

		System.out.print("send task: " + subtaskIndex + " key: " + value.f0 + " state: " + localAverage.value());
		if (localAverage.value().f1 != 0) {
			System.out.println(" avg: " + (localAverage.value().f0 / localAverage.value().f1));
		}
		
		recordCount++;

		deltaSum += value.f1;
		deltaN++;

		if (recordCount == synchronizationStepSize) {
			for (int i = 0; i < parallelism; i++) {
				if (i != value.f0) {
					out.collect(new Tuple3<Long, Integer, Integer>((long)i, deltaSum, deltaN));
				}
			}

			recordCount = 0;
			
			//clean delta
			deltaSum = 0;
			deltaN = 0;
		}
		}

	@Override
	public void flatMap2(Tuple3<Long,Integer, Integer> value, Collector<Tuple3<Long,Integer,Integer>> out) throws Exception {
		System.out.println("receive task: " + subtaskIndex + " key: " + value.f0);
		
		Tuple2<Integer, Integer> avg = localAverage.value();
		if (value.f0 != subtaskIndex) {
			localAverage.update(new Tuple2<Integer, Integer>(avg.f0 + value.f1, avg.f1 + value.f2));
		}

		System.out.print("task: " + subtaskIndex + " state: " + localAverage.value());
		if (localAverage.value().f1 != 0) {
			System.out.println(" avg: " + (localAverage.value().f0 / localAverage.value().f1));
		}
	}
}
