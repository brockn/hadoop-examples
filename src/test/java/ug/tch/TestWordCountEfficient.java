package ug.tch;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.junit.Test;
public class TestWordCountEfficient {
  LongWritable offset = new LongWritable(0);
  Mapper<LongWritable, Text, Text, LongWritable> mapper = new WordCountMapper();
  Reducer<Text, LongWritable, LongWritable, Text> reducer = new LongSumInvertedOutputReducer();
  Reducer<Text, LongWritable, Text, LongWritable> combiner = new LongSumReducer<Text>(); 
  MapDriver<LongWritable, Text, Text, LongWritable> mapDriver = 
    new MapDriver<LongWritable, Text, Text, LongWritable>(mapper);
  MapReduceDriver<LongWritable, Text, Text, LongWritable, LongWritable, Text> mapReduceDriver = 
      new MapReduceDriver<LongWritable, Text, Text, LongWritable, LongWritable, Text>(mapper, reducer, combiner);
  @Test
  public void testMapper() throws IOException {
    mapDriver.resetOutput();
    mapDriver.withInput(offset, new Text("Cat cat dog"));
    mapDriver.withOutput(new Text("cat"), new LongWritable(1));
    mapDriver.withOutput(new Text("cat"), new LongWritable(1));
    mapDriver.withOutput(new Text("dog"), new LongWritable(1));
    mapDriver.runTest();
  }
  
  @Test
  public void testMapReduce() throws IOException {
    mapReduceDriver.resetOutput();
    mapReduceDriver.withInput(offset, new Text("Cat cat dog"));
    mapReduceDriver.withOutput(new LongWritable(2), new Text("cat"));
    mapReduceDriver.withOutput(new LongWritable(1), new Text("dog"));
    mapReduceDriver.runTest();
  }
}