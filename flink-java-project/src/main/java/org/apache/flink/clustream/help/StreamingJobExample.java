package org.apache.flink.clustream.help;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.clustream.functions.*;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.ml.math.Vector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.File;


/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a full example of a Flink Streaming Job, see the SocketTextStreamWordCount.java
 * file in the same package/directory or have a look at the website.
 *
 * You can also generate a .jar file that you can submit on your Flink
 * cluster.
 * Just type
 * 		mvn clean package
 * in the projects root directory.
 * You will find the jar in
 * 		target/flink-java-project-0.1.jar
 * From the CLI you can then run
 * 		./bin/flink run -c org.apache.flink.clustream.StreamingJob target/flink-java-project-0.1.jar
 *
 * For more information on the CLI see:
 *
 * http://flink.apache.org/docs/latest/apis/cli.html
 */
public class StreamingJobExample {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 2);
		conf.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 2);
		conf.setString(ConfigConstants.AKKA_ASK_TIMEOUT, "2 h");

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(4, conf);
		
		DataStream<Tuple2<Integer,Integer>> stream = env.fromElements(new Tuple2<Integer,Integer>(1,1), new Tuple2<Integer,Integer>(1,1), 
			new Tuple2<Integer,Integer>(2,1), new Tuple2<Integer,Integer>(2,1));
		
		//stream.keyBy(0).map(new MapSum()).setParallelism(4).print();

		//stream.map(new MapSumStateless()).print();
		
		//stream.keyBy(0).countWindow(1L).apply(new SumWindowFunction()).print();

		//stream.keyBy(0).countWindow(1L).reduce(new SumReduceFunction2()).print();

		stream.keyBy(0).reduce(new SumReduceFunction2()).setParallelism(4).print();
		
		
		
		
		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}
