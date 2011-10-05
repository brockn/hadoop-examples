package ug.tch;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.junit.Test;
public class TestWordCountInefficient {
  LongWritable offset = new LongWritable(0);
  Mapper<LongWritable, Text, Text, LongWritable> mapper = new WordCountMapper();
  Reducer<Text, LongWritable, LongWritable, Text> reducer = new LongSumReducerInvertedOutput();
  MapDriver<LongWritable, Text, Text, LongWritable> driver = 
    new MapDriver<LongWritable, Text, Text, LongWritable>(mapper);
  MapReduceDriver<LongWritable, Text, Text, LongWritable, LongWritable, Text> mapReduceDriver = 
      new MapReduceDriver<LongWritable, Text, Text, LongWritable, LongWritable, Text>(mapper, reducer);
  @Test
  public void testMapper() throws IOException {
    driver.resetOutput();
    driver.withInput(offset, new Text("Cat cat dog"));
    driver.withOutput(new Text("cat"), new LongWritable(1));
    driver.withOutput(new Text("cat"), new LongWritable(1));
    driver.withOutput(new Text("dog"), new LongWritable(1));
    driver.runTest();
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