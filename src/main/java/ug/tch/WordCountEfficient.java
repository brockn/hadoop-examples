package ug.tch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountEfficient extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    int rc = ToolRunner.run(new WordCountEfficient(), args);
    System.exit(rc);
  }

  public int run(String[] args) throws Exception {
    if(args.length < 2) {
      throw new Exception("Usage: " + this.getClass().getName() + " output input [input...]");
    }
    Configuration conf = getConf();
    Path output = new Path(args[0]);
    JobConf job = new JobConf(conf);
    job.setJarByClass(WordCountEfficient.class);
    job.setMapperClass(WordCountMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);
    job.setCombinerClass(LongSumReducer.class);
    job.setReducerClass(LongSumReducerInvertedOutput.class);
    FileOutputFormat.setOutputPath(job, output);
    for (int i = 1; i < args.length; i++) {
      Path input = new Path(args[i]);
      FileInputFormat.addInputPath(job, input);
    }
    JobClient.runJob(job);
    return 0;
  }
}
