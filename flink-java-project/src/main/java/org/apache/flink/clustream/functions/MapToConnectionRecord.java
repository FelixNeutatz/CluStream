package org.apache.flink.clustream.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.clustream.StreamingJob;
import org.apache.flink.clustream.data.ConnectionRecord;

/**
 * Created by felix on 08.11.16.
 */
public class MapToConnectionRecord implements MapFunction<String, ConnectionRecord> {
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
