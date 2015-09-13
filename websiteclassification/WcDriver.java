package websiteclassification;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.*;

public class WcDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		    if (args.length != 2) {
		      System.out.printf(
		          "Usage: WordCount <input dir> <output dir>\n");
		      System.exit(-1);
		    }

		    Job job = new Job();
		    job.setJarByClass(WcDriver.class);
		    job.setJobName("Website Classification");
		    
		    FileInputFormat.setInputPaths(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    
		    job.setMapperClass(WcMapper.class);
		    job.setReducerClass(WcReducer.class);

		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    
		    boolean success = job.waitForCompletion(true);
		    if (success) {
		    	long health_c = job.getCounters().findCounter("classification", "health").getValue();
		    	long ecommerce_c = job.getCounters().findCounter("classification", "ecommerce").getValue();
		    	long education_c = job.getCounters().findCounter("classification", "education").getValue();
		    	long entertainment_c = job.getCounters().findCounter("classification", "entertainment").getValue();
		    	
		    	System.out.println("health   = " + health_c);
		        System.out.println("ecommerce   = " + ecommerce_c);
		        System.out.println("education = " + education_c);
		        System.out.println("entertainment = " + entertainment_c);
		        
		        HashMap<String, Long> hm = new HashMap<String, Long>();
		        hm.put("health", new Long(health_c));
		        hm.put("ecommerce", new Long(ecommerce_c));
		        hm.put("education", new Long(education_c));
		        hm.put("entertainment", new Long(entertainment_c));
		        
		        // Get a set of the entries
		        Set set = hm.entrySet();
		        // Get an iterator
		        Iterator i = set.iterator();
		        // Display elements
		        Long max = new Long(-1);
		        String categ = "";
		        while(i.hasNext()) {
		           Map.Entry me = (Map.Entry)i.next();
		           System.out.println(me.getKey() + ": " + me.getValue());
		           if((Long)me.getValue() >= max){
		        	   max = (Long)me.getValue();
		        	   categ = (String)me.getKey();
		           }
		           
		        }
		        
		        /*long [] categ = new long [] {health_c, ecommerce_c, education_c, entertainment_c};
		        long max = -1;
		        for(long v : categ){
		        	if(v >= max)
		        		max = v;
		        }
		        */
		        System.out.println("The category of this site is: " + categ);
		    	return 0;
		    }
		    else
		    	return 1;
    	
	}
	public static void main(String[] args) throws Exception {
	    int exitCode = ToolRunner.run(new Configuration(), new WcDriver(), args);
	    System.exit(exitCode);
	}
}
