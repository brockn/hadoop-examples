package ug.tch;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Grep extends Configured implements Tool {

  public static enum Counters {
    NO_MATCH
  }
 public static class InnerMapper extends MapReduceBase implements
 Mapper<Object, Text, Text, NullWritable> {
   String term;
   @Override
   public void configure(JobConf job) {
     term = job.get("grep.term");
     if(term == null) {
       throw new RuntimeException("grep.term config param not found");
     }
     term = term.toLowerCase();
   }
   public void map(Object key, Text value,
       OutputCollector<Text, NullWritable> output, Reporter report)
   throws IOException {
     String line = value.toString().toLowerCase();
     if(line.indexOf(term) >= 0) {
       output.collect(value, NullWritable.get());
     } else {
       report.incrCounter(Counters.NO_MATCH, 1);
     }
   }
 }


 public int run(String[] args) throws Exception {
   if(args.length < 2) {
     throw new Exception("Usage: " + this.getClass().getName() + " output term input [input...]");
   }
   Configuration conf = getConf();
   Path output = new Path(args[0]);
   conf.set("grep.term", args[1]);
   JobConf job = new JobConf(conf);
   job.setJarByClass(Grep.class);
   job.setMapperClass(InnerMapper.class);
   job.setMapOutputKeyClass(Text.class);
   job.setMapOutputValueClass(NullWritable.class);
   job.setReducerClass(IdentityReducer.class);
   job.setMapOutputKeyClass(Text.class);
   job.setMapOutputValueClass(NullWritable.class);
   FileOutputFormat.setOutputPath(job, output);
   for (int i = 1; i < args.length; i++) {
     Path input = new Path(args[i]);
     FileInputFormat.addInputPath(job, input);
   }
   JobClient.runJob(job);
   return 0;
 }


 public static void main(String[] args) throws Exception {
   Grep tool = new Grep();
   int rc = ToolRunner.run(tool, args);
   System.exit(rc);
 }
}