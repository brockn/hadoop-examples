package ug.tch;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class KeyValueTextNumericSort extends Configured implements Tool {

  public static enum Counters {
    BAD_INPUT
  };
  public static class InnerMapper extends MapReduceBase implements
      Mapper<Text, Text, DoubleWritable, Text> {

    private DoubleWritable outputKey = new DoubleWritable();

    public void map(Text key, Text value,
        OutputCollector<DoubleWritable, Text> output, Reporter report)
        throws IOException {
        try {
          outputKey.set(Double.parseDouble(key.toString()));
          output.collect(outputKey, value);
        } catch (NumberFormatException e) {
          report.incrCounter(Counters.BAD_INPUT, 1);
        }
    }
  }

  /**
   * For illustrative purposes only, should use 
   * org.apache.hadoop.mapred.lib.IdentityReducer
   */
  public static class InnerReducer extends MapReduceBase implements
      Reducer<Writable, Writable, Writable, Writable> {

    public void reduce(Writable key, Iterator<Writable> values,
        OutputCollector<Writable, Writable> output, Reporter reporter)
        throws IOException {
      while (values.hasNext()) {
        output.collect(key, values.next());
      }
    }
  }

  public static void main(String[] args) throws Exception {
    int rc = ToolRunner.run(new KeyValueTextNumericSort(), args);
    System.exit(rc);
  }

  public int run(String[] args) throws Exception {
    if(args.length < 2) {
      throw new Exception("Usage: " + this.getClass().getName() + " output input [input...]");
    }
    Configuration conf = getConf();
    Path output = new Path(args[0]);
    JobConf job = new JobConf(conf);
    job.setJarByClass(KeyValueTextNumericSort.class);
    job.setMapperClass(InnerMapper.class);
    job.setInputFormat(KeyValueTextInputFormat.class);
    job.setMapOutputKeyClass(DoubleWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(DoubleWritable.class);
    job.setOutputValueClass(Text.class);
    job.setReducerClass(InnerReducer.class);
    FileOutputFormat.setOutputPath(job, output);
    for (int i = 1; i < args.length; i++) {
      Path input = new Path(args[i]);
      FileInputFormat.addInputPath(job, input);
    }
    JobClient.runJob(job);
    return 0;
  }
}
