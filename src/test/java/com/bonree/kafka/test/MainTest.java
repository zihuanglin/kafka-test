package com.bonree.kafka.test;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.junit.Ignore;
import org.junit.Test;

public class MainTest {

	@Test @Ignore
	public void testCSV() throws Exception {
		String[] headers = new String[] {"num", "batch.size", "linger.ms", "buffer.memory", "compression.type", "写入tps", "写入Mb/s", "最大延迟"};
		
		Object[] data = new Object[] {"0", "16384", 0, "32m", "none", 99999, "32M", 1000};
		
	    FileWriter out = new FileWriter("F:\\data\\kafka\\book_new.csv", true);
	    
	    CSVFormat format = CSVFormat.DEFAULT.withHeader(headers).withSkipHeaderRecord().withHeaderComments("create by liulin", new Date()).withCommentMarker('\t');
	    
	    try (CSVPrinter printer = new CSVPrinter(out, format)) {
	    	printer.printRecord(data);
	    }
	    
	}
	
	@Test @Ignore
	public void testReadLine() throws IOException {
		String output = "Reading payloads from: /data/br/base/sdk.data10w\r\n" + 
				"Number of messages read: 100000\r\n" + 
				"499702 records sent, 99940.4 records/sec (37.41 MB/sec), 283.4 ms avg latency, 862.0 ms max latency.\r\n" + 
				"500368 records sent, 100073.6 records/sec (37.44 MB/sec), 3.0 ms avg latency, 23.0 ms max latency.\r\n" + 
				"500044 records sent, 100008.8 records/sec (37.41 MB/sec), 2.4 ms avg latency, 20.0 ms max latency.\r\n" + 
				"500232 records sent, 100046.4 records/sec (37.40 MB/sec), 2.7 ms avg latency, 43.0 ms max latency.\r\n" + 
				"499956 records sent, 99991.2 records/sec (37.39 MB/sec), 3.7 ms avg latency, 111.0 ms max latency.\r\n" + 
				"500239 records sent, 100047.8 records/sec (37.45 MB/sec), 2.3 ms avg latency, 17.0 ms max latency.\r\n" + 
				"485298 records sent, 97059.6 records/sec (36.31 MB/sec), 9.4 ms avg latency, 239.0 ms max latency.\r\n" + 
				"465072 records sent, 93014.4 records/sec (34.80 MB/sec), 750.9 ms avg latency, 2346.0 ms max latency.\r\n" + 
				"549878 records sent, 109975.6 records/sec (41.14 MB/sec), 109.0 ms avg latency, 894.0 ms max latency.\r\n" + 
				"499926 records sent, 99985.2 records/sec (37.42 MB/sec), 2.3 ms avg latency, 20.0 ms max latency.\r\n" + 
				"500222 records sent, 100044.4 records/sec (37.44 MB/sec), 2.4 ms avg latency, 30.0 ms max latency.\r\n" + 
				"500005 records sent, 100001.0 records/sec (37.40 MB/sec), 2.3 ms avg latency, 15.0 ms max latency.\r\n" + 
				"500278 records sent, 100055.6 records/sec (37.41 MB/sec), 2.1 ms avg latency, 19.0 ms max latency.\r\n" + 
				"499486 records sent, 99877.2 records/sec (37.33 MB/sec), 12.2 ms avg latency, 367.0 ms max latency.\r\n" + 
				"500774 records sent, 100154.8 records/sec (37.42 MB/sec), 169.9 ms avg latency, 1255.0 ms max latency.\r\n" + 
				"499999 records sent, 99999.8 records/sec (37.42 MB/sec), 22.2 ms avg latency, 421.0 ms max latency.\r\n" + 
				"500087 records sent, 99997.4 records/sec (37.39 MB/sec), 5.8 ms avg latency, 203.0 ms max latency.\r\n" + 
				"500171 records sent, 100034.2 records/sec (37.44 MB/sec), 2.3 ms avg latency, 20.0 ms max latency.\r\n" + 
				"500003 records sent, 100000.6 records/sec (37.41 MB/sec), 2.1 ms avg latency, 12.0 ms max latency.\r\n" + 
				"10000000 records sent, 99978.004839 records/sec (37.40 MB/sec), 67.82 ms avg latency, 2346.00 ms max latency, 2 ms 50th, 538 ms 95th, 1331 ms 99th, 2130 ms 99.9th";
		
		InputStream inputStream = new ByteArrayInputStream(output.getBytes());
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
		
		String last = null;
		for(String line; (line = reader.readLine()) != null;) {
			last = line;
		}
		
		System.out.println(last);
		String[] args = last.split(",");
		
		for(String arg : args) {
			System.out.println(arg);
		}
		
		String arg1 = args[1].replaceAll("records/sec", "").replaceAll("\\(", "").replaceAll("MB/sec\\)", "").trim();
		System.out.println(arg1);
		String[] arg1s = arg1.split("  ");
		
		double recordsPerSec = Double.parseDouble(arg1s[0]);
		double bytesPerSec = Double.parseDouble(arg1s[1]);
		System.out.println(recordsPerSec + " " + bytesPerSec);
		
		
		String arg7 = args[7].replaceAll("ms 99.9th", "").trim();
		System.out.println(arg7);
	}
	
	@Test @Ignore
	public void stringArrayCopy() {
		String[] args1 = new String[] {"hello", "world"};
		
		String[] args2 = new String[] {"java", "string"};
		
		 List<String> list = new ArrayList<String>();
		 list.addAll(Arrays.<String>asList(args1));
		 list.addAll(Arrays.<String>asList(args2));
		String[] args3 = list.toArray(new String[4]);
		
		for(String s : args3) {
			System.out.println(s);
		}
	}
	
	@Test @Ignore
	public void testGetFileName() {
		System.out.println(Main.getFileName());
	}
	
	@Test @Ignore
	public void testCreateCSVFile() throws IOException {
		String[] headers = new String[] {"num", "batch.size", "linger.ms", "buffer.memory", "compression.type", "写入tps", "写入Mb/s", "最大延迟"};
		Object[] data = new Object[] {"0", "16384", 0, "32m", "none", 99999, "32M", 1000};
		
		String fileName = "test-csv.csv";
		Main.createCSVFile(headers, data, fileName);
	}
	
	@Test
	public void testCalcTotal() {
		List<Object[]> records = new ArrayList<>();
		records.add(new Object[] {"0", "16384", 0, "32m", "none", 10, 5, 1000});
		records.add(new Object[] {"0", "16384", 0, "32m", "none", 18, 6, 2000});
		Object[] result = Main.calcTotal(records, 5, 3);
		
		assertEquals(3, result.length);
		assertEquals(28D, result[0]);
		assertEquals(11D, result[1]);
		assertEquals(2000D, result[2]);
	}
	
}
