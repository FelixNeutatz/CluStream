package org.apache.flink.clustream.data;

/**
 * Created by felix on 08.11.16.
 */
public class ConnectionRecord {
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
