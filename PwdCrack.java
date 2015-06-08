package hadoop.pwdcrack;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 * @author	Yuancheng Zhang
 * @propose	CSC439 Computer Security - Final Paper
 * @date	06/01/2015
 */

public class PwdCrack {
	
	// the Hash value
	public static String knowHash = "daa854b44775bc8bbcc68e7ccb50b0aa";
	// the algorithm name
	public static String algoName = getAlgorithmName(knowHash);

	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(0);
		private Text word = new Text();
		
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String line = value.toString();
			line = line.replaceAll("(\\r|\\n|\\s+)", "");
			word.set(line);
			byte[] gPass = line.getBytes();
			if (crackPassword(gPass, knowHash, algoName)){
				one.set(1);
				output.collect(word, one);
			}
		}
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}
	
	public static String getAlgorithmName(String str) {
		switch (str.length()){
		case 32:
			return "MD5";	
		case 40: 
			return "SHA-1";
		case 64: 
			return "SHA-256";
		default:
			throw new RuntimeException();
		}
	}
	
	/*
	 * Crack password code is writen by Skyler Kaufman 
	 * in CSC439 Assignment_3, Group 3rd_Bravo
	 */
	public static boolean crackPassword(byte[] password, String knownHash, String algoName) {
		try {
			MessageDigest md = MessageDigest.getInstance(algoName);
			md.reset();
			md.update(password);
			byte[] byteStr = md.digest();

			StringBuffer sb = new StringBuffer();
			for (int i = 0; i < byteStr.length; i++) {
				/*
				 * Conversion of byte [] to hex taken from
				 * http://www.mkyong.com/java/java-sha-hashing-example/
				 */
				sb.append(Integer.toString((byteStr[i] & 0xff) + 0x100, 16)
						.substring(1));
			}
			String hashString = sb.toString();
			// String hashString = new BigInteger(1, md.digest()).toString(16);
			if (hashString.equals(knownHash)) {
				return true;
			}

		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return false;
	}
	
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(PwdCrack.class);
		conf.setJobName("pwdcrack");
		conf.setInt("dfs.block.size", 50*1024*1024);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		knowHash = args[2];
		algoName = getAlgorithmName(knowHash);
		
		JobClient.runJob(conf);
	}
}

/*
cd ~/local_data/
rm -r ~/local_data/PwdCrack
mkdir ~/local_data/PwdCrack

rm -r ~/local_data/PwdCrack/*
javac -classpath ~/hadoop/hadoop-core-1.2.1.jar -d ./PwdCrack PwdCrack.java
jar -cvf ~/local_data/PwdCrack.jar -C PwdCrack/ .
hadoop fs -rmr /user/ubuntu/data/output
hadoop jar ~/local_data/PwdCrack.jar hadoop.pwdcrack.PwdCrack /user/ubuntu/data/input /user/ubuntu/data/output

hadoop fs -lsr /user/ubuntu/data/output/
hadoop fs -cat /user/ubuntu/data/output/part-00000 | more
 */

