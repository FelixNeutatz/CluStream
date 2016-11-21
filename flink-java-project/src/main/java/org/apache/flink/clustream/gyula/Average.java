package org.apache.flink.clustream.gyula;

/**
 * Created by felix on 15.11.16.
 */
public class Average {
	public int sum;
	public int N;
	public int subtaskIndex;
	
	public Average(){
		
	}
	
	public Average(int subtaskIndex) {
		this.sum = 0;
		this.N = 0;
		this.subtaskIndex = subtaskIndex;
	}
	
	@Override
	public String toString(){
		float avg = 0;
		if (N > 0)
			avg = sum / N;
		return "Task (" + subtaskIndex + "): Average: " + sum + " / " + N + " = " + avg;
	}
}
