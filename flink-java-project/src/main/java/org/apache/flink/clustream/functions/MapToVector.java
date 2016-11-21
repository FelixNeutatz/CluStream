package org.apache.flink.clustream.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.ml.math.DenseVector;
import org.apache.flink.ml.math.Vector;

/**
 * Created by felix on 08.11.16.
 */
public class MapToVector implements MapFunction<String, Vector> {
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
