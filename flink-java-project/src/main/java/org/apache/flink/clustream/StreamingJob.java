package org.apache.flink.clustream;

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


import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.clustream.functions.CountWindowLength;
import org.apache.flink.clustream.functions.MapToIndexTimeVector;
import org.apache.flink.clustream.functions.Splitter;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.ml.math.Vector;
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
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		File file = new File(classLoader.getResource("data/kddcup.newtestdata_small_unlabeled_index").getFile());

		DataStreamSource<String> source = env.readTextFile(file.getPath());

		DataStream<Tuple3<Long,Long,Vector>> connectionRecords = source.map(new MapToIndexTimeVector());

		//use first records to train kmeans Model
		SplitStream<Tuple3<Long,Long,Vector>> split = connectionRecords.split(new Splitter(311073L));

		DataStream<Tuple3<Long,Long,Vector>> kmeansData = split.select("Kmeans");



		DataStream<Tuple3<Long,Long,Vector>> cluData = split.select("CluStream");


		cluData.keyBy(0).window(TumblingProcessingTimeWindows.of(Time.milliseconds(100))).apply (
			new CountWindowLength()).writeAsCsv("test_output", FileSystem.WriteMode.OVERWRITE);
		
		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}
