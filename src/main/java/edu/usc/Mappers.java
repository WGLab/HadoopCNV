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

public class Mappers{
  // we can put any common utilities in here
}

class BinMapper
extends Mapper<LongWritable,Text,
               RefBinKey,Text> {
  private final RefBinKey refBinKey = new RefBinKey();
  private final int bin_length = PennCnvSeq.bin_length;


  private final Text newVal = new Text();
  @Override protected void map(LongWritable inkey,Text inval,
                               Mapper<LongWritable,Text,
                               RefBinKey,Text>.Context ctx)
    throws InterruptedException, IOException{
      String []parts = inval.toString().split("\t");
      String refname = parts[0];
      int bin = (int)(Integer.parseInt(parts[1])/bin_length);
      String binMapValue = StringUtils.join(parts,"\t",1,4);
      //System.err.println("BinMap value: "+binMapValue);
      refBinKey.setRefName(refname);
      refBinKey.setBin(bin);
      newVal.set(binMapValue);
      ctx.write(refBinKey,newVal);  
     
    }
}

class BinSortMapper
extends Mapper<LongWritable,Text,
               RefBinKey,Text> {
  private final Text newVal = new Text();
  @Override protected void map(LongWritable inkey,Text inval,
                               Mapper<LongWritable,Text,
                               RefBinKey,Text>.Context ctx)
    throws InterruptedException, IOException{
      String []parts = inval.toString().split("\t");
      String refname = parts[0];
      int bin = Integer.parseInt(parts[1]);
      String value = StringUtils.join(parts,"\t",2,parts.length);
      //System.err.println("BinMap value: "+binMapValue);
      newVal.set(value);
      ctx.write(new RefBinKey(refname,bin),newVal);  
     
    }
}

class SAMRecordMapper
extends Mapper<LongWritable,SAMRecordWritable,
               RefPosBaseKey,DoubleWritable> {

  VcfLookup lookup = null;
  public SAMRecordMapper(){
    lookup = new VcfLookup();
    lookup.readObject();
  }

  @Override protected void map(LongWritable inkey,SAMRecordWritable inval,
                               Mapper<LongWritable,SAMRecordWritable,
                               RefPosBaseKey,DoubleWritable>.Context ctx)
    throws InterruptedException, IOException{
      SAMRecord samrecord = inval.get();    
      String refname = samrecord.getReferenceName();
      //String samstr = samrecord.getSAMString();
      //System.err.println("SAMSTRING: "+samstr);
      //System.err.println(" readlen: "+samrecord.getReadLength());
      byte[] bases = samrecord.getReadBases();
      byte[] basequals = samrecord.getBaseQualities();
      int mapqual = samrecord.getMappingQuality();
      double mapprob = 1.-Math.pow(10,-mapqual*.1);
      //System.err.println(" mapquality "+mapprob);
      //System.err.println(" bytelen: "+bases.length);
      //System.err.println(" first base: "+Byte.toString(bases[0]));
      //System.err.println(" cigar: "+samrecord.getCigar().toString());
      //System.err.println(" quality: "+samrecord.getMappingQuality());
      List<AlignmentBlock> alignmentBlockList = samrecord.getAlignmentBlocks();
      Iterator<AlignmentBlock> it = alignmentBlockList.iterator();
      while(it.hasNext()){
        AlignmentBlock alignmentBlock = it.next();
        //System.err.println("  block start "+alignmentBlock.getReferenceStart()+"("+alignmentBlock.getReadStart()+") with length "+alignmentBlock.getLength());
        int refstart = alignmentBlock.getReferenceStart();
        int readstart = alignmentBlock.getReadStart();
        int alignlen = alignmentBlock.getLength();
        //char []basechars = new char[alignlen];
        for(int i=0;i<alignlen;++i){
          int refpos = refstart + i;
          if(lookup.exists(refname,refpos)){
          // Let's break up the string into chunks of 10
          //if(refpos % PennCnvSeq.base_spacing == 0){
            int base = (int)bases[readstart+i-1];
            //basechars[i] = (char)base;
            int basequal = (int)basequals[readstart+i-1];
            double baseprob = 1.-Math.pow(10,-basequal*.1);
            //System.err.println(" base "+base+" quality: "+(1.-Math.pow(10,-basequal*.1)));
            double fullprob = mapprob;
            //double fullprob = mapprob*baseprob;
            if(fullprob>.99) {
              ctx.write(new RefPosBaseKey(refname,refstart+i,base),new DoubleWritable(fullprob));  
            }
          }
        }
      }
    }
}
