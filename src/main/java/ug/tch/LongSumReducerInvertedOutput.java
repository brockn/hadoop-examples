package ug.tch;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * For illustrative purposes only, should use 
 * org.apache.hadoop.mapred.lib.LongSumReducer
 */
public class LongSumReducerInvertedOutput extends MapReduceBase implements
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