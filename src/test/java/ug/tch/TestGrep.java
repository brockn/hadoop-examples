package ug.tch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;
public class TestGrep {
  Text noMatch = new Text("This does not match");
  Text match = new Text("This line is a MATCH");
  LongWritable offset = new LongWritable(0);
  String term = "MATCH";
  Mapper<LongWritable, Text, Text, NullWritable> mapper = new Grep.InnerMapper();
  JobConf conf = new JobConf();
  MapDriver<LongWritable, Text, Text, NullWritable> driver = 
    new MapDriver<LongWritable, Text, Text, NullWritable>(mapper);

  @Before
  public void setup() {
    conf.set("grep.term", term);
    mapper.configure(conf);
    
  }
  @Test
  public void testMatch() throws IOException {
    driver.resetOutput();
    driver.withInput(offset, match);
    List<Pair<Text, NullWritable>> result = driver.run();
    assertTrue("Expcted 1 result, got " + result.size(), result.size() == 1);
    Pair<Text, NullWritable> pair = result.get(0);
    assertEquals("Expected " + match + " and got " + pair.getFirst(),
        pair.getFirst(), match);
  }
  @Test
  public void testNoMatch() throws IOException {
    driver.resetOutput();
    driver.withInput(offset, noMatch);
    List<Pair<Text, NullWritable>> result = driver.run();
    assertTrue("Expcted 0 result, got " + result.size(), result.size() == 0);
  }
}