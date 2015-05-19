package edu.usc;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobFilter;
//import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.AlignmentBlock;

import org.seqdoop.hadoop_bam.AnySAMInputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;


public class Sharders{
}

class SortRecordReader extends RecordReader<RefPosBaseKey,Text>{

 /* methods are delegated to this variable
 *
 */
  private final RecordReader<LongWritable, Text> reader;

  private final RefPosBaseKey currentKey = new RefPosBaseKey();
  private final Text currentValue = new Text();

  public SortRecordReader(final RecordReader<LongWritable,Text>reader)
  throws IOException{
    this.reader = reader;
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  @Override 
  public RefPosBaseKey getCurrentKey(){ 
    return currentKey;
  }

  @Override 
  public Text getCurrentValue(){ 
    return currentValue;
  }

  @Override
  public float getProgress() throws IOException,InterruptedException {
    return reader.getProgress();
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) 
  throws IOException,InterruptedException {
    reader.initialize(split,context);
  }

  @Override
  public boolean nextKeyValue()
  throws IOException,InterruptedException{
    boolean result = reader.nextKeyValue();
    if(!result){
      return false;
    }
    Text lineRecordReaderValue = reader.getCurrentValue();

    extractKey(lineRecordReaderValue);
    currentValue.set(extractValue(lineRecordReaderValue));
    return true;
  }

  private void extractKey(final Text value)
  throws IOException{
    String[] parts = value.toString().split("\t");
    //int binRange = (int)(Integer.parseInt(parts[1]) / bin_length);
    //System.err.println("BINRANGE for "+parts[1]+" is "+binRange);
    //RefPosBaseKey newKey = new RefPosBaseKey(parts[0],binRange);
    //RefPosBaseKey newKey = new RefPosBaseKey(parts[0],Integer.parseInt(parts[1]));
    currentKey.setRefName(parts[0]);
    currentKey.setPosition(Integer.parseInt(parts[1]));
    currentKey.setBase(Integer.parseInt(parts[2]));
    //return newKey;
  }

  private String extractValue(final Text value)
  throws IOException{
    String[] parts = value.toString().split("\t");
    //String newVal = StringUtils.join(parts,"\t",2,6);
    return parts[3];
  }
}

class SortInputFormat
extends InputFormat {

  private final TextInputFormat inputFormat = new TextInputFormat();

  @Override
  public List<InputSplit> getSplits(JobContext context)
  throws IOException,InterruptedException{
    return inputFormat.getSplits(context);
  }

  @Override
  public RecordReader<RefPosBaseKey,Text> createRecordReader(final InputSplit genericSplit, TaskAttemptContext context)
  throws IOException,InterruptedException {
    context.setStatus(genericSplit.toString());
    return new SortRecordReader(inputFormat.createRecordReader(genericSplit,context));
  }
}

