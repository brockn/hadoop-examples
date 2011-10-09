package ug.tch;

//old vs new api
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


/*
 * Mapper input types are LongWritable and Text
 *  - Reading from plain text file, key is byte offset, value is line
 *  - Writables are mutable
 *  - No state *should* be kept in mapper
 */
public class WordCountMapper extends MapReduceBase implements
    Mapper<LongWritable, Text, Text, LongWritable> {
  
  private Pattern wordFinder = Pattern.compile("\\W+");

  private LongWritable outputValue = new LongWritable(1);

  private Text outputKey = new Text();

  public void map(LongWritable key, Text value,
      OutputCollector<Text, LongWritable> output, 
      Reporter report)
      throws IOException {
    String[] words = wordFinder.split(value.toString());
    for (int i = 0; i < words.length; i++) {
      String word = words[i].toLowerCase();
      if (word.length() > 0) {
        outputKey.set(word);
        output.collect(outputKey, outputValue);
      }
    }
  }
}