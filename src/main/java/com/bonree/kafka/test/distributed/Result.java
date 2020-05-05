package com.bonree.kafka.test.distributed;

import java.util.List;

public class Result {
	private int completeThread;
	private List<Object> records;
	
	public int getCompleteThread() {
		return completeThread;
	}
	public void setCompleteThread(int completeThread) {
		this.completeThread = completeThread;
	}
	public List<Object> getRecords() {
		return records;
	}
	public void setRecords(List<Object> records) {
		this.records = records;
	}
	
}
