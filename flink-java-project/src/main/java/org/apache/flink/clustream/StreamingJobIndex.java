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


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.ml.math.DenseVector;
import org.apache.flink.ml.math.Vector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;


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
public class StreamingJobIndex {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		File file = new File(classLoader.getResource("data/kddcup.newtestdata_small_unlabeled_index").getFile());
		
		DataStreamSource<String> source = env.readTextFile(file.getPath());

		DataStream<Tuple2<Long,Vector>> connectionRecords = source.map(new MapToVector()).setParallelism(4);

		//csv to windows
		connectionRecords.keyBy(0).countWindow(2).apply (
			new WindowFunction<Tuple2<Long,Vector>, Tuple1<Integer>, Tuple, GlobalWindow>() {
				public void apply (Tuple tuple,
								   GlobalWindow window,
								   Iterable<Tuple2<Long, Vector>> values,
								   Collector<Tuple1<Integer>> out) throws Exception {
					int sum = 0;
					Iterator iterator = values.iterator();
					while (iterator.hasNext () ) {
						Tuple2<Long,Vector> t = (Tuple2<Long,Vector>)iterator.next();
						sum += 1;
					}
					out.collect (new Tuple1<Integer>(new Integer(sum)));
				}
		}).writeAsCsv("test_output", FileSystem.WriteMode.OVERWRITE);

		// execute program
		env.execute("CluStream");
	}

	
	public static class MapToVector implements MapFunction<String, Tuple2<Long,Vector>> {
		@Override
		public Tuple2<Long,Vector> map(String value) throws Exception {
			String [] splits = value.split(",");
			double [] values = new double [34];
			values[0]  = Float.parseFloat(splits[1]);
			values[1]  = Float.parseFloat(splits[5]);
			values[2]  = Float.parseFloat(splits[6]);
			values[3]  = Float.parseFloat(splits[8]);
			values[4]  = Float.parseFloat(splits[9]);
			values[5]  = Float.parseFloat(splits[10]);
			values[6]  = Float.parseFloat(splits[11]);
			values[7]  = Float.parseFloat(splits[13]);
			values[8]  = Float.parseFloat(splits[14]);
			values[9]  = Float.parseFloat(splits[15]);
			values[10] = Float.parseFloat(splits[16]);
			values[11] = Float.parseFloat(splits[17]);
			values[12] = Float.parseFloat(splits[18]);
			values[13] = Float.parseFloat(splits[19]);
			values[14] = Float.parseFloat(splits[20]);
			values[15] = Float.parseFloat(splits[23]);
			values[16] = Float.parseFloat(splits[24]);
			values[17] = Float.parseFloat(splits[25]);
			values[18] = Float.parseFloat(splits[26]);
			values[19] = Float.parseFloat(splits[27]);
			values[20] = Float.parseFloat(splits[28]);
			values[21] = Float.parseFloat(splits[29]);
			values[22] = Float.parseFloat(splits[30]);
			values[23] = Float.parseFloat(splits[31]);
			values[24] = Float.parseFloat(splits[32]);
			values[25] = Float.parseFloat(splits[33]);
			values[26] = Float.parseFloat(splits[34]);
			values[27] = Float.parseFloat(splits[35]);
			values[28] = Float.parseFloat(splits[36]);
			values[29] = Float.parseFloat(splits[37]);
			values[30] = Float.parseFloat(splits[38]);
			values[31] = Float.parseFloat(splits[39]);
			values[32] = Float.parseFloat(splits[40]);
			values[33] = Float.parseFloat(splits[41]);
			
			System.out.println(Arrays.toString(values));

			return new Tuple2<Long, Vector>(Long.parseLong(splits[0]), new DenseVector(values));
		}
	}
}
