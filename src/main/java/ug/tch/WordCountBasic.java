package ug.tch;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountBasic extends Configured implements Tool {

  public static class InnerMapper extends MapReduceBase implements
      Mapper<LongWritable, Text, Text, LongWritable> {
    private Pattern wordFinder = Pattern.compile("\\W+");

    private LongWritable outputValue = new LongWritable(1);

    private Text outputKey = new Text();

    public void map(LongWritable key, Text value,
        OutputCollector<Text, LongWritable> output, Reporter report)
        throws IOException {
      String[] words = wordFinder.split(value.toString());
      for (int i = 0; i < words.length; i++) {
        String word = words[i];
        if (word.length() > 0) {
          outputKey.set(word);
          output.collect(outputKey, outputValue);
        }
      }
    }
  }

  /**
   * For illustrative purposes only, should use 
   * org.apache.hadoop.mapred.lib.LongSumReducer
   */
  public static class InnerReducer extends MapReduceBase implements
      Reducer<Text, LongWritable, LongWritable, Text> {
    private LongWritable outputValue = new LongWritable();

    public void reduce(Text word, Iterator<LongWritable> values,
        OutputCollector<LongWritable, Text> output, Reporter reporter)
        throws IOException {
      long count = 0L;
      while (values.hasNext()) {
        LongWritable value = values.next();
        count += value.get();
      }
      outputValue.set(count);
      output.collect(outputValue, word);
    }
  }

  public static void main(String[] args) throws Exception {
    int rc = ToolRunner.run(new WordCountBasic(), args);
    System.exit(rc);
  }

  public int run(String[] args) throws Exception {
    if(args.length < 2) {
      throw new Exception("Usage: " + this.getClass().getName() + " output input [input...]");
    }
    Configuration conf = getConf();
    Path output = new Path(args[0]);
    JobConf job = new JobConf(conf);
    job.setJarByClass(WordCountBasic.class);
    job.setMapperClass(InnerMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);
    job.setOutputKeyClass(LongWritable.class);
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
