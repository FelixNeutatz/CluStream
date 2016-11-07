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
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.ml.math.DenseVector;
import org.apache.flink.ml.math.Vector;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.io.File;
import java.net.URL;
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
public class StreamingJob {

	public static class ConnectionRecord {
		public float duration;
		public String protocol_type;
		public String service;
		public String flag;
		public float src_bytes;
		public float dst_bytes;
		public String land;
		public float wrong_fragment;
		public float urgent;
		public float hot;
		public float num_failed_logins;
		public String logged_in;
		public float num_compromised;
		public float root_shell;
		public float su_attempted;
		public float num_root;
		public float num_file_creations;
		public float num_shells;
		public float num_access_files;
		public float num_outbound_cmds;
		public String is_host_login;
		public String is_guest_login;
		public float count;
		public float srv_count;
		public float serror_rate;
		public float srv_serror_rate;
		public float rerror_rate;
		public float srv_rerror_rate;
		public float same_srv_rate;
		public float diff_srv_rate;
		public float srv_diff_host_rate;
		public float dst_host_count;
		public float dst_host_srv_count;
		public float dst_host_same_srv_rate;
		public float dst_host_diff_srv_rate;
		public float dst_host_same_src_port_rate;
		public float dst_host_srv_diff_host_rate;
		public float dst_host_serror_rate;
		public float dst_host_srv_serror_rate;
		public float dst_host_rerror_rate;
		public float dst_host_srv_rerror_rate;
	}

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		
		URL url = Thread.currentThread().getContextClassLoader().getResource("/data/kddcup.newtestdata_10_percent_unlabeled");
		//File file = new File(url.getPath());
		File file = new File("/home/felix/streams/CluStream/flink-java-project/src/main/resources/data/kddcup.newtestdata_10_percent_unlabeled");
		
		DataStreamSource<String> source = env.readTextFile(file.getPath());

		DataStream<Vector> connectionRecords = source.map(new MapToVector());

		DataStream<Tuple2<Integer,Vector>> connectionRecordsT = connectionRecords.map(new MapToTuple());
		
		/*
		connectionRecordsT.keyBy(0).timeWindow(Time.milliseconds(1000L)).apply (new WindowFunction<Tuple2<Integer,Vector>, Integer, Tuple, Window>() {
			public void apply (Tuple tuple,
							   Window window,
							   Iterable<Tuple2<Integer, Vector>> values,
							   Collector<Integer> out) throws Exception {
				int sum = 0;
				Iterator iterator = values.iterator();
				while (iterator.hasNext () ) {
					sum += 1;
				}
				out.collect (new Integer(sum));
			}
		}).print();*/

		/*
		AllWindowedStream windowedStream = connectionRecords.windowAll(TumblingProcessingTimeWindows.of(Time.milliseconds(100)));

		SingleOutputStreamOperator countWindowElements = windowedStream.apply (new AllWindowFunction<Vector, Integer, Window>() {
			public void apply (Window window,
							   Iterable<Vector> values,
							   Collector<Integer> out) throws Exception {
				int sum = 0;
				Iterator iterator = values.iterator();
				while (iterator.hasNext () ) {
					sum += 1;
				}
				out.collect (new Integer(sum));
			}
		}).setParallelism(4);
		
		countWindowElements.print();*/
		//connectionRecords.print();

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}

	public static class MapToConnectionRecord implements MapFunction<String, ConnectionRecord> {
		@Override
		public ConnectionRecord map(String value) throws Exception {
			String [] splits = value.split(",");
			ConnectionRecord record = new ConnectionRecord();
			record.duration = Float.parseFloat(splits[0]);
			record.protocol_type = splits[1];
			record.service = splits[2];
			record.flag = splits[3];
			record.src_bytes = Float.parseFloat(splits[4]);
			record.dst_bytes = Float.parseFloat(splits[5]);
			record.land = splits[6];
			record.wrong_fragment = Float.parseFloat(splits[7]);
			record.urgent = Float.parseFloat(splits[8]);
			record.hot = Float.parseFloat(splits[9]);
			record.num_failed_logins = Float.parseFloat(splits[10]);
			record.logged_in = splits[11];
			record.num_compromised = Float.parseFloat(splits[12]);
			record.root_shell = Float.parseFloat(splits[13]);
			record.su_attempted = Float.parseFloat(splits[14]);
			record.num_root = Float.parseFloat(splits[15]);
			record.num_file_creations = Float.parseFloat(splits[16]);
			record.num_shells = Float.parseFloat(splits[17]);
			record.num_access_files = Float.parseFloat(splits[18]);
			record.num_outbound_cmds = Float.parseFloat(splits[19]);
			record.is_host_login = splits[20];
			record.is_guest_login = splits[21];
			record.count = Float.parseFloat(splits[22]);
			record.srv_count = Float.parseFloat(splits[23]);
			record.serror_rate = Float.parseFloat(splits[24]);
			record.srv_serror_rate = Float.parseFloat(splits[25]);
			record.rerror_rate = Float.parseFloat(splits[26]);
			record.srv_rerror_rate = Float.parseFloat(splits[27]);
			record.same_srv_rate = Float.parseFloat(splits[28]);
			record.diff_srv_rate = Float.parseFloat(splits[29]);
			record.srv_diff_host_rate = Float.parseFloat(splits[30]);
			record.dst_host_count = Float.parseFloat(splits[31]);
			record.dst_host_srv_count = Float.parseFloat(splits[32]);
			record.dst_host_same_srv_rate = Float.parseFloat(splits[33]);
			record.dst_host_diff_srv_rate = Float.parseFloat(splits[34]);
			record.dst_host_same_src_port_rate = Float.parseFloat(splits[35]);
			record.dst_host_srv_diff_host_rate = Float.parseFloat(splits[36]);
			record.dst_host_serror_rate = Float.parseFloat(splits[37]);
			record.dst_host_srv_serror_rate = Float.parseFloat(splits[38]);
			record.dst_host_rerror_rate = Float.parseFloat(splits[39]);
			record.dst_host_srv_rerror_rate = Float.parseFloat(splits[40]);
			
			return record;
		}
	}

	public static class MapToVector implements MapFunction<String, Vector> {
		@Override
		public Vector map(String value) throws Exception {
			String [] splits = value.split(",");
			double [] values = new double [34];
			values[0]  = Float.parseFloat(splits[0]);
			values[1]  = Float.parseFloat(splits[4]);
			values[2]  = Float.parseFloat(splits[5]);
			values[3]  = Float.parseFloat(splits[7]);
			values[4]  = Float.parseFloat(splits[8]);
			values[5]  = Float.parseFloat(splits[9]);
			values[6]  = Float.parseFloat(splits[10]);
			values[7]  = Float.parseFloat(splits[12]);
			values[8]  = Float.parseFloat(splits[13]);
			values[9]  = Float.parseFloat(splits[14]);
			values[10] = Float.parseFloat(splits[15]);
			values[11] = Float.parseFloat(splits[16]);
			values[12] = Float.parseFloat(splits[17]);
			values[13] = Float.parseFloat(splits[18]);
			values[14] = Float.parseFloat(splits[19]);
			values[15] = Float.parseFloat(splits[22]);
			values[16] = Float.parseFloat(splits[23]);
			values[17] = Float.parseFloat(splits[24]);
			values[18] = Float.parseFloat(splits[25]);
			values[19] = Float.parseFloat(splits[26]);
			values[20] = Float.parseFloat(splits[27]);
			values[21] = Float.parseFloat(splits[28]);
			values[22] = Float.parseFloat(splits[29]);
			values[23] = Float.parseFloat(splits[30]);
			values[24] = Float.parseFloat(splits[31]);
			values[25] = Float.parseFloat(splits[32]);
			values[26] = Float.parseFloat(splits[33]);
			values[27] = Float.parseFloat(splits[34]);
			values[28] = Float.parseFloat(splits[35]);
			values[29] = Float.parseFloat(splits[36]);
			values[30] = Float.parseFloat(splits[37]);
			values[31] = Float.parseFloat(splits[38]);
			values[32] = Float.parseFloat(splits[39]);
			values[33] = Float.parseFloat(splits[40]);

			return new DenseVector(values);
		}
	}

	public static class MapToTuple implements MapFunction<Vector, Tuple2<Integer, Vector>> {
		@Override
		public Tuple2<Integer, Vector> map(Vector value) throws Exception {
			return new Tuple2<Integer, Vector>(ThreadLocalRandom.current().nextInt(0, 1000000000),value);
		}
	}
	
	
}
