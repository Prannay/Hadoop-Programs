package identitymapper;

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

public class IdMapper extends Configured implements Tool {
	@Override
	  public int run(String[] args) throws Exception {

	    if (args.length != 2) {
	      System.out.printf("Usage: IdMapper <input dir> <output dir>\n");
	      return -1;
	    }

	    Job job = new Job(getConf());
	    job.setJarByClass(IdMapper.class);
	    job.setJobName("IdMapper");

	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	   
	    job.setNumReduceTasks(0);

	    return job.waitForCompletion(true) ? 0:1;
	  
	  }

	  public static void main(String[] args) throws Exception {
	    int exitCode = ToolRunner.run(new Configuration(), new IdMapper(), args);
	    System.exit(exitCode);
	  }

}
