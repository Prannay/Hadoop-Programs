package identityreducer;

import identitymapper.IdMapper;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class IdReducer extends Configured implements Tool{
	@Override
	  public int run(String[] args) throws Exception {

	    if (args.length != 2) {
	      System.out.printf("Usage: IdReducer <input dir> <output dir>\n");
	      return -1;
	    }

	    Job job = new Job(getConf());
	    job.setJarByClass(IdReducer.class);
	    job.setJobName("IdReducer");

	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setMapperClass(AvglenMapper.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);

	    return job.waitForCompletion(true) ? 0:1;
	  
	  }

	  public static void main(String[] args) throws Exception {
	    int exitCode = ToolRunner.run(new Configuration(), new IdReducer(), args);
	    System.exit(exitCode);
	  }
}
