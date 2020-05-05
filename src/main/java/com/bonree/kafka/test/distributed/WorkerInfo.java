package com.bonree.kafka.test.distributed;

public class WorkerInfo {
	private String hostname;
	private int capacity;
	
	public WorkerInfo() {
	}
	
	public WorkerInfo(String hostname, int capacity) {
		this.hostname = hostname;
		this.capacity = capacity;
	}
	
	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public int getCapacity() {
		return capacity;
	}

	public void setCapacity(int capacity) {
		this.capacity = capacity;
	}

	@Override
	public String toString() {
		return "WorkerInfo [hostname=" + hostname + ", capacity=" + capacity + "]";
	}
}
