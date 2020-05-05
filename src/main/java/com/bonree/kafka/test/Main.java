package com.bonree.kafka.test;

import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import com.alibaba.fastjson.JSONObject;
import com.bonree.kafka.test.distributed.Leader;
import com.bonree.kafka.test.distributed.LeaderSelector;
import com.bonree.kafka.test.distributed.Worker;
import com.bonree.kafka.test.distributed.WorkerInfo;
import com.bonree.kafka.test.distributed.WorkerMonitor;

/**
 * 	本工具类用于测试kafka性能,<br>
 * 	涉及到的参数有batch.size,linger.ms,buffer.memory和compression.type等
 * 	通过遍历各个参数的值生成测试参数, 执行kafka自带命令{@code kafka-producer-perf-test.sh}
 *	解析输出的日志信息获取tps等指标, 输出到csv文件中
 * @author Administrator
 *
 */
public class Main {
	public static final String[] HEADERS = new String[] {"num", "thread.count", "batch.size", "linger.ms", "buffer.memory", "compression.type", "写入tps", "写入Mb/s", "99.9分位延迟时间"};

	public static void main(String[] args) throws IOException {
		System.out.println("start");
		List<Closeable> closes = new ArrayList<>();
		String fileName = getFileName();
		Runnable clearTask = () -> {
			 System.out.println(" Running shutdown hook");
			 for(int i = closes.size() - 1; i >= 0; i--) {
				 try {
					 closes.get(i).close();
					} catch (IOException e) {
						e.printStackTrace();
					}
			 }
		};
		//捕获Ctrl+C信号
		final Thread hook = new Thread(clearTask);
		Runtime.getRuntime().addShutdownHook(hook);

		try {
			Config config = getConfig(args);
			System.out.println("fileName: " + fileName + " config: " + config);
			String hostname = InetAddress.getLocalHost().getHostAddress();
		
			RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,3);
			CuratorFramework client = CuratorFrameworkFactory.newClient(config.getZookeeperHosts(), retryPolicy);
			client.start();
			closes.add(() -> client.close());

			if("master".equals(config.getServiceType())) {
				LeaderSelector selector = new LeaderSelector(client, hostname);
				List<String> tasks = getTasks(config, fileName);
				Leader leader = new Leader(client, selector, tasks);
				leader.start();
				closes.add(() -> leader.stop());
			}

			WorkerMonitor wokerMonitor = new WorkerMonitor(client, new WorkerInfo(hostname, config.getCapacity()));
			Worker woker = new Worker(config, wokerMonitor);
			woker.start();
			closes.add(() -> woker.stop());

			Thread.sleep(Integer.MAX_VALUE);
		} catch(Throwable e) {
			e.printStackTrace();
		}finally {
			Runtime.getRuntime().removeShutdownHook(hook);
			clearTask.run();
		}
	}

	public static String getFileName() {
		String uuid = UUID.randomUUID().toString();
		SimpleDateFormat format = new SimpleDateFormat("MMdd");
		return String.format("performance_%s_%s.csv", format.format(new Date()), uuid.substring(0, uuid.indexOf('-')));
	}
	

	/**
	 *  创建CSV文件
	 * @param script
	 * @param record
	 * @param append
	 * @throws IOException
	 */
	public static void createCSVFile(String[] headers, Object[] record, String fileName) throws IOException {
		File file = new File(fileName);
		boolean append = file.exists();
		if(!append) {
			System.err.println(String.format("file[%s] not exist", fileName));
		}
			
	    FileWriter out = new FileWriter(file, true);
	    CSVFormat format = CSVFormat.DEFAULT.withHeader(headers).withSkipHeaderRecord(append);
	    try (CSVPrinter printer = new CSVPrinter(out, format)) {
	    	printer.printRecord(record);
	    }
	    System.out.println(file.getAbsolutePath());
	}
	
	/**
	 *	计算多个值的总和
	 * @param lists
	 * @param begin Object[]数组的起始位置
	 * @param length Object[]数组长度
	 * @return
	 */
	public static Object[] calcTotal(List<Object[]> lists, int begin, int length) {
		if(begin < 0) {
			throw new IllegalArgumentException("beagin must > 0 but get " + begin);
		}
		
		Object[] total = new Object[length];
		for(int i = 0; i < lists.size(); i++) {
			Object[] s = lists.get(i);
			
			if((begin + length) > s.length) {
				throw new IllegalArgumentException("beagin + length must < Object[].length! begin=" + begin +" length=" + begin);
			}
			
			for(int j = 0; j < total.length; j++) {
				Object var = s[begin + j];
				//total为空, 直接存入值
				if(total[j] == null) {
					total[j] = var;
				}else {
					//tps和Mb/s列直接相加, 延迟时间取最大值
					if(j < 2) {
						total[j] = Double.parseDouble(total[j].toString()) +  Double.parseDouble(var.toString());
					}else {
						total[j] = Math.max(Double.parseDouble(total[j].toString()), Double.parseDouble(var.toString()));
					}
				}
			}
		}
		return total;
	}
	
	/**
	 * 	 生成task协议
	 * @param config
	 * @param fileName
	 * @return
	 */
	public static List<String> getTasks(Config config, String fileName) {
		List<String> tasks = new ArrayList<>();
		
		String bootstrapServers = config.getBootstrapServers();
		String script = config.getScript();
		int index = 0;
		for(int threadCountIndex = 0; threadCountIndex < config.getThreadCount().size(); threadCountIndex ++) {
			int threadCount = config.getThreadCount().get(threadCountIndex);
			
			//compressionIndex,bufferMemIndex,lingerIndex, batchSizeIndex
			for(int compressionIndex = 0 ; compressionIndex < config.getCompressionType().size(); compressionIndex++) {
				String compressionType = config.getCompressionType().get(compressionIndex);
				
				for(int bufferMemIndex = 0; bufferMemIndex < config.getBufferMemory().size(); bufferMemIndex++) {
					int bufferMemory = config.getBufferMemory().get(bufferMemIndex);
					
					for(int lingerIndex = 0; lingerIndex < config.getLingerMs().size(); lingerIndex++) {
						int lingerMs = config.getLingerMs().get(lingerIndex);
						
						for(int batchSizeIndex = 0; batchSizeIndex < config.getBatchSize().size(); batchSizeIndex++) {
							int batchSize = config.getBatchSize().get(batchSizeIndex);
							//从指定的次数开始
							if(index++ < config.getNum() && config.getNum() > 0) {
								continue;
							}
							JSONObject object = new JSONObject();
							object.put("index", index);
							object.put("threadCount", threadCount);
							object.put("fileName", fileName);
							
							JSONObject command = new JSONObject();
							command.put("script", script);
							command.put("num-records", config.getNumRecords());
							command.put("throughput", config.getThroughput());
							command.put("topic", config.getTopic());
							command.put("payload-file", config.getPayloadFile());
							
							JSONObject props = new JSONObject();
							props.put("bootstrap.servers", bootstrapServers);
							props.put("batch.size", batchSize);
							props.put("linger.ms", lingerMs);
							props.put("buffer.memory", bufferMemory);
							props.put("compression.type", compressionType);
							command.put("producer-props", props);
							
							object.put("command", command);
							System.out.println(String.format("index: %s, thread count: %s script: %s", index, threadCount, command));
							tasks.add(object.toJSONString());
						}
					}
				}
			}
		}
		return tasks;
	}

	private static Config getConfig(String[] args) throws Exception {
		if(args.length < 1) {
			throw new RuntimeException("USAGE: java -jar xxx.jar config.properties [num (the num to begin only when exit && > 0)]");
		}
		String propertiesPath = args[0];
		Properties properties = new Properties();
		try {
			properties.load(new FileReader(propertiesPath));
		}catch(Exception e) {
			throw new RuntimeException("properties file not exit! : " + propertiesPath, e);
		}
		int num = 0;
		if(args.length == 2) {
			num = Integer.parseInt(args[1]);
		}
		Config config = new Config();
		config.setServiceType(properties.getProperty("service.type"));
		config.setCapacity(getIntValue(properties, "woker.capacity"));
		config.setZookeeperHosts(properties.getProperty("zookeeper.host"));
		config.setNum(num);
		config.setScript(properties.getProperty("script"));
		config.setThreadCount(string2IntList(properties, "thread.count"));
		config.setNumRecords(getIntValue(properties, "num.records"));
		config.setThroughput(getIntValue(properties, "throughput"));
		config.setPayloadFile(properties.getProperty("payload.file"));
		config.setTopic(properties.getProperty("topic"));
		config.setBootstrapServers(properties.getProperty("bootstrap.servers"));
		config.setBatchSize(string2IntList(properties, "batch.size"));
		config.setLingerMs(string2IntList(properties, "linger.ms"));
		config.setBufferMemory(string2IntList(properties, "buffer.memory"));
		config.setCompressionType(string2StringList(properties, "compression.type"));
		config.setKafkaHeapOpts(properties.getProperty("KAFKA_HEAP_OPTS"));
		return config;
	}

	private static int getIntValue(Properties properties, String name) {
		String value = properties.getProperty(name);
		if(value == null) {
			throw new RuntimeException("properties key is null! key: " + name);
		}
		return Integer.parseInt(value);
	}

	private static List<Integer> string2IntList(Properties properties, String name){
		String value = properties.getProperty(name);
		
		if(value == null || value.isEmpty()) {
			throw new RuntimeException("properties key is null! key: " + name);
		}
		
		List<Integer> ints = new ArrayList<Integer>();
		try {
			for(String s : value.split(",")) {
				ints.add(Integer.parseInt(s));
			}
		}catch(Exception e) {
			throw new RuntimeException("parse properties error! key: " + name, e);
		}
		return ints;
	}

	private static List<String> string2StringList(Properties properties, String name){
		String value = properties.getProperty(name);
		
		if(value == null || value.isEmpty()) {
			throw new RuntimeException("properties key is null! key: " + name);
		}
		
		List<String> strs = null;
		try {
			strs = Arrays.asList(value.split(","));
		}catch(Exception e) {
			throw new RuntimeException("parse properties error! key: " + name, e);
		}
		return strs;
	}
}
