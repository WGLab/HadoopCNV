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

public class Keys{
}

class RefRangeAlignmentKey implements
WritableComparable<RefRangeAlignmentKey>{

  private String refname;
  private int position1;
  private int position2;
  private String bases;

  public RefRangeAlignmentKey(){}

  public RefRangeAlignmentKey(String refname,int position1,int position2, String bases){
    this.refname = refname;
    this.position1 = position1;
    this.position2 = position2;
    this.bases = bases;
  }

  public void setRefName(String refname){
    this.refname = refname;
  }
  
  public void setPosition1(int position1){
    this.position1 = position1;
  }

  public void setPosition2(int position2){
    this.position2 = position2;
  }

  public void setBase(String bases){
    this.bases = bases;
  }

  public void write(DataOutput out) throws IOException {
    out.writeUTF(refname);
    out.writeInt(position1);
    out.writeInt(position2);
    out.writeUTF(bases);
  }

  public void readFields(DataInput in) throws IOException {
    refname = in.readUTF();
    position1 = in.readInt();
    position2 = in.readInt();
    bases = in.readUTF();
  }

  public int compareTo(RefRangeAlignmentKey other) {
    int cmp = refname.compareTo(other.refname);
    if(cmp!=0) return cmp;

    if (this.position1<other.position1) return -1;
    else if (this.position1>other.position1) return 1;
    else if (this.position2<other.position2) return -1;
    else if (this.position2>other.position2) return 1;
    else return bases.compareTo(other.bases);
  }

  @Override public String toString(){
    return refname+"\t"+Integer.toString(position1)+"\t"+Integer.toString(position2)+"\t"+bases;
  }

  @Override public int hashCode(){
    final int prime = 31;
    int result = 1;
    result = prime * result + ((refname==null) ? 0 : refname.hashCode());
    result = prime * result + (int)(position1 ^ (position1 >>> 32));
    result = prime * result + (int)(position2 ^ (position2 >>> 32));
    result = prime * result + ((bases==null) ? 0 : bases.hashCode());
    return result;
  }
}

class RefPosBaseKey implements
WritableComparable<RefPosBaseKey>{

  private String refname;
  private int position;
  private int base;

  public RefPosBaseKey(){}
  public RefPosBaseKey(String refname,int position, int base){
    this.refname = refname;
    this.position = position;
    this.base = base;
  }

  public void setRefName(String refname){
    this.refname = refname;
  }
  
  public void setPosition(int position){
    this.position = position;
  }

  public void setBase(int base){
    this.base = base;
  }

  public void write(DataOutput out) throws IOException {
    out.writeUTF(refname);
    out.writeInt(position);
    out.writeInt(base);
  }

  public void readFields(DataInput in) throws IOException {
    refname = in.readUTF();
    position = in.readInt();
    base = in.readInt();
  }

  public int compareTo(RefPosBaseKey other) {
    int cmp = refname.compareTo(other.refname);
    if(cmp!=0) return cmp;

    if (this.position<other.position) return -1;
    else if (this.position>other.position) return 1;
    else if (this.base<other.base) return -1;
    else if (this.base>other.base) return 1;
    else return 0;
  }

  @Override public String toString(){
    return refname+"\t"+Integer.toString(position)+"\t"+Integer.toString(base);
  }
  @Override public int hashCode(){
    final int prime = 31;
    int result = 1;
    result = prime * result + ((refname==null) ? 0 : refname.hashCode());
    result = prime * result + (int)(position ^ (position >>> 32));
    result = prime * result + base;
    return result;
  }
}

class RefBinKey implements
WritableComparable<RefBinKey>{

  private String refname;
  private int bin;

  public RefBinKey(){}
  public RefBinKey(String refname,int bin){
    this.refname = refname;
    this.bin = bin;
  }

  public void setRefName(String refname){
    this.refname = refname;
  }

  public void setBin(int bin){
    this.bin = bin;
  }
  public void write(DataOutput out) throws IOException {
    out.writeUTF(refname);
    out.writeInt(bin);
  }
  public String getRefName(){
    return refname;
  }

  public void readFields(DataInput in) throws IOException {
    refname = in.readUTF();
    bin = in.readInt();
  }

  public int compareTo(RefBinKey other) {
    int cmp = refname.compareTo(other.refname);
    if(cmp!=0) return cmp;
    else if (this.bin<other.bin) return -1;
    else if (this.bin>other.bin) return 1;
    else return 0;
  }

  @Override public String toString(){
    return refname+"\t"+Integer.toString(bin);
  }
  @Override public int hashCode(){
    final int prime = 31;
    int result = 1;
    result = prime * result + ((refname==null) ? 0 : refname.hashCode());
    result = prime * result + bin;
    return result;
  }
}
