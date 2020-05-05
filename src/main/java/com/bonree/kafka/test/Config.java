package com.bonree.kafka.test;

import java.util.List;

public class Config {
	private String zookeeperHosts;
	private int num;
	private List<Integer> threadCount;
	private String script;
	private int numRecords;
	private String payloadFile;
	private int throughput;
	private String topic;
	private String bootstrapServers;
	private List<Integer> batchSize;
	private List<Integer> lingerMs;
	private List<Integer> bufferMemory;
	private List<String> compressionType;
	private String kafkaHeapOpts;
	private String serviceType;
	private int capacity;
	
	public int getNum() {
		return num;
	}
	public void setNum(int num) {
		this.num = num;
	}
	public List<Integer> getThreadCount() {
		return threadCount;
	}
	public void setThreadCount(List<Integer> threadCount) {
		this.threadCount = threadCount;
	}
	public String getScript() {
		return script;
	}
	public void setScript(String script) {
		this.script = script;
	}
	public int getNumRecords() {
		return numRecords;
	}
	public void setNumRecords(int numRecords) {
		this.numRecords = numRecords;
	}
	public String getPayloadFile() {
		return payloadFile;
	}
	public void setPayloadFile(String payloadFile) {
		this.payloadFile = payloadFile;
	}
	public int getThroughput() {
		return throughput;
	}
	public void setThroughput(int throughput) {
		this.throughput = throughput;
	}
	public String getTopic() {
		return topic;
	}
	public void setTopic(String topic) {
		this.topic = topic;
	}
	public String getBootstrapServers() {
		return bootstrapServers;
	}
	public void setBootstrapServers(String bootstrapServers) {
		this.bootstrapServers = bootstrapServers;
	}
	public List<Integer> getBatchSize() {
		return batchSize;
	}
	public void setBatchSize(List<Integer> batchSize) {
		this.batchSize = batchSize;
	}
	public List<Integer> getLingerMs() {
		return lingerMs;
	}
	public void setLingerMs(List<Integer> lingerMs) {
		this.lingerMs = lingerMs;
	}
	public List<Integer> getBufferMemory() {
		return bufferMemory;
	}
	public void setBufferMemory(List<Integer> bufferMemory) {
		this.bufferMemory = bufferMemory;
	}
	public List<String> getCompressionType() {
		return compressionType;
	}
	public void setCompressionType(List<String> compressionType) {
		this.compressionType = compressionType;
	}
	public String getKafkaHeapOpts() {
		return kafkaHeapOpts;
	}
	public void setKafkaHeapOpts(String kafkaHeapOpts) {
		this.kafkaHeapOpts = kafkaHeapOpts;
	}
	public String getServiceType() {
		return serviceType;
	}
	public void setServiceType(String serviceType) {
		this.serviceType = serviceType;
	}
	public int getCapacity() {
		return capacity;
	}
	public void setCapacity(int capacity) {
		this.capacity = capacity;
	}
	public String getZookeeperHosts() {
		return zookeeperHosts;
	}
	public void setZookeeperHosts(String zookeeperHosts) {
		this.zookeeperHosts = zookeeperHosts;
	}
	@Override
	public String toString() {
		return "Config [zookeeperHosts=" + zookeeperHosts + ", num=" + num + ", threadCount=" + threadCount + ", script=" + script
				+ ", numRecords=" + numRecords + ", payloadFile=" + payloadFile + ", throughput=" + throughput + ", topic=" + topic
				+ ", bootstrapServers=" + bootstrapServers + ", batchSize=" + batchSize + ", lingerMs=" + lingerMs + ", bufferMemory="
				+ bufferMemory + ", compressionType=" + compressionType + ", kafkaHeapOpts=" + kafkaHeapOpts + ", serviceType="
				+ serviceType + ", capacity=" + capacity + "]";
	}
}
