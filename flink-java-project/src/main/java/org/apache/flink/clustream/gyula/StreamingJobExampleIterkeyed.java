package org.apache.flink.clustream.gyula;

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
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
public class StreamingJobExampleIterkeyed {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 2);
		conf.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 2);
		conf.setString(ConfigConstants.AKKA_ASK_TIMEOUT, "2 h");
		
		int parallelism = 4;
		int stepSize = 1;

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(parallelism, conf);

		/*
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		File file = new File(classLoader.getResource("data/test1.csv").getFile());

		DataStreamSource<String> source = env.readTextFile(file.getPath());

		DataStream<Tuple2<Long,Integer>> input = source.map(new MapToTuple2()).setParallelism(parallelism); */
		
		DataStream<Tuple2<Long,Integer>> input = env.addSource(new MySource(parallelism)).setParallelism(parallelism);
		
		IterativeStream.ConnectedIterativeStreams<Tuple2<Long,Integer>, Tuple3<Long,Integer,Integer>> inputsAndCentroids = 
			input.iterate().withFeedbackType("Tuple3<Long,Integer,Integer>"); // Tuple3<Key, SUM, N>

		DataStream<Tuple3<Long,Integer,Integer>> updatedCentroids = inputsAndCentroids
			.keyBy(0,0).flatMap(new MyCoFlatmap2(stepSize)).setParallelism(parallelism);
		
		inputsAndCentroids.closeWith(updatedCentroids);

		
		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}
