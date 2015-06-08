package hadoop.pwdcrack;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 * @author Yuancheng Zhang
 * @propose CSC439 Computer Security - Final Paper
 * @date 06/01/2015
 */

public class PwdCrack {

	// the Hash value
	public static String knowHash = "0eb5e03d3cff86a8fa09d6280a6ee4771156d91e";
	// the algorithm name
	public static String algoName = getAlgorithmName(knowHash);

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		private final static Text algorithm = new Text();
		private final static Text hashValue = new Text();
		private Text dictItem = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			line = line.replaceAll("(\\r|\\n|\\s+)", "");
			dictItem.set(line);
			byte[] gPass = line.getBytes();
			if (crackPassword(gPass, knowHash, algoName)) {
				algorithm.set(algoName);
				output.collect(dictItem, algorithm);
				hashValue.set(knowHash);
				output.collect(dictItem, hashValue);
			}
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String algoName = "";
			while (values.hasNext()){
				algoName = algoName + values.next().toString() + "\t";
			}
			output.collect(key, new Text(algoName));
		}
	}

	public static String getAlgorithmName(String str) {
		switch (str.length()) {
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
	 * Crack password code is writen by Skyler Kaufman in CSC439 Assignment_3,
	 * Group 3rd_Bravo
	 */
	public static boolean crackPassword(byte[] password, String knownHash,
			String algoName) {
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
		conf.setInt("dfs.block.size", 50 * 1024 * 1024);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}

/*
 * cd ~/local_data/ rm -r ~/local_data/PwdCrack mkdir ~/local_data/PwdCrack
 * 
 * 
rm -r ~/local_data/PwdCrack/*
javac -classpath ~/hadoop/hadoop-core-1.2.1.jar -d ./PwdCrack PwdCrack.java
jar -cvf ~/local_data/PwdCrack.jar -C PwdCrack/ .
hadoop fs -rmr /user/ubuntu/data/output
hadoop jar ~/local_data/PwdCrack.jar hadoop.pwdcrack.PwdCrack /user/ubuntu/data/input /user/ubuntu/data/output 
 
hadoop fs -lsr /user/ubuntu/data/output/ 
hadoop fs -cat /user/ubuntu/data/output/part-00000 | more
 */

