package websiteclassification;

import java.io.IOException;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.Properties;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class WcReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		System.err.println("INSIDE REDUCE");
		int wordCount = 0;
		for (IntWritable value : values) {
			wordCount += value.get();
		}
		
		Properties prop = new Properties();
		InputStream input = null;
		
		try {

			input = new FileInputStream("/home/cloudera/workspace/training/bin/websiteclassification/keyword.properties");

			// load a properties file
			prop.load(input);

			Enumeration<?> e = prop.propertyNames();
			while (e.hasMoreElements()) {
				String ky = (String) e.nextElement();
				String val = prop.getProperty(ky);
				System.out.println("Key : " + ky + ", Value : " + val);
				String [] arr = val.split(",");
				if ( ArrayUtils.contains( arr, key.toString() ) ) {
				    context.getCounter("classification", ky).increment(wordCount);
				}
			}

		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		context.write(key, new IntWritable(wordCount));
	}
	/*public static void main(String [] args) throws Exception {
		System.out.println("HELLO");
		Properties prop = new Properties();
		InputStream input = null;
		
		try {

			input = new FileInputStream("/home/cloudera/workspace/training/bin/websiteclassification/keyword.properties");

			// load a properties file
			prop.load(input);

			Enumeration<?> e = prop.propertyNames();
			while (e.hasMoreElements()) {
				String ky = (String) e.nextElement();
				String val = prop.getProperty(ky);
				System.out.println("Key : " + ky + ", Value : " + val);
				String [] arr = val.split(",");
				//if ( ArrayUtils.contains( arr, "id" ) ) {
				//    context.getCounter("classification", ky).increment(wordCount);
				//}
			}

		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}*/
}
