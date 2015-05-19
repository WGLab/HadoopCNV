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

public class Reducers{
}

class BinReducer
extends Reducer<RefBinKey,Text,
               RefBinKey,Text> {
  
  private final Text retText = new Text();
  private final Map<Integer,List<Float> > binMap = new HashMap();
  private final Set<Float> depthSet = new TreeSet();
  private final Set<Float> bafSet = new TreeSet();

  @Override 
  protected void reduce(RefBinKey inkey,Iterable<Text> invals,
  Reducer<RefBinKey,Text, RefBinKey,Text>.Context ctx)
  throws InterruptedException, IOException{
    Iterator<Text> it = invals.iterator();
    binMap.clear();
    int startbp = Integer.MAX_VALUE;
    int endbp = Integer.MIN_VALUE;
    while(it.hasNext()){
      Text text = it.next();
      String[] parts = text.toString().split("\t");
      int p=0;
      Integer bp = Integer.decode(parts[p++]);
      if(bp>endbp)endbp = bp;
      if(bp<startbp)startbp = bp;
      int allele = Integer.parseInt(parts[p++]);
      Float depth = Float.valueOf(parts[p++]);
      if (binMap.get(bp)==null){
        binMap.put(bp,new java.util.ArrayList());
      }
      binMap.get(bp).add(depth);
      //System.err.println("BIN REDUCER: "+inkey.toString()+" : "+bp+" : "+depth);
    }
    Iterator<Integer> it_keys = binMap.keySet().iterator();
    depthSet.clear();
    bafSet.clear();
    double total_depth = 0;
    double total_baf = 0;
    int n=0;
    int hets = 0;
    int baf_n = 0;
    while(it_keys.hasNext()){
      Integer bp = it_keys.next();
      Iterator<Float> depthListIterator = binMap.get(bp).iterator();
      float maxDepth = 0,pos_depth=0;
      while(depthListIterator.hasNext()){
        float depth = depthListIterator.next().floatValue();
        pos_depth+=depth;
        if(depth>maxDepth) maxDepth = depth;
      }
      float baf = (pos_depth-maxDepth)/pos_depth;
      //if(pos_depth<20 || baf<0.1) baf = 0f;
      if(baf > 0f){ 
        //System.err.println("BIN REDUCER adding at "+bp+" : "+pos_depth+","+baf);
        ++hets;
        bafSet.add(baf);
        total_baf+=baf;
        ++baf_n;
      }
      depthSet.add(pos_depth);
      total_depth+=pos_depth;
      ++n;
    }
    double mean_depth=(n>0)?total_depth/n:0;
    double mean_baf=(baf_n>0)?total_baf/baf_n:0;
    Iterator<Float> depthSetIt = depthSet.iterator();
    Iterator<Float> bafSetIt = bafSet.iterator();
    float medianDepth = 0;
    float medianBaf = 0;
    for(int i=0;i<depthSet.size()/2;++i){
      medianDepth = depthSetIt.next();
    }
    for(int i=0;i<bafSet.size()/2;++i){
      medianBaf = bafSetIt.next();
    }
    //System.err.println("BIN REDUCER: depthsetsize: "+depthSet.size()+" bafsetsize: "+bafSet.size()+" median depth: "+medianDepth+" median baf: "+medianBaf);
    // for median depth
    retText.set(Integer.toString(startbp)+"\t"+Integer.toString(endbp)+"\t"+Double.toString(mean_depth)+"\t"+Double.toString(mean_baf)+"\t"+Double.toString(total_depth)+"\t"+Integer.toString(n));
    //retText.set(Integer.toString(startbp)+"\t"+Integer.toString(endbp)+"\t"+Double.toString(medianDepth)+"\t"+Integer.toString(hets)+"\t"+Double.toString(total_depth)+"\t"+Integer.toString(n));
    ctx.write(inkey, retText);
  }
}

class CnvReducer
extends Reducer<RefBinKey,Text,
               Text,Text> {
  private final Text outKey = new Text();
  private final Text textRes = new Text();
  @Override 
  protected void reduce(RefBinKey inkey,Iterable<Text> invals,
  Reducer<RefBinKey,Text, Text,Text>.Context ctx)
  throws InterruptedException, IOException{
    Iterator<Text> it_text = invals.iterator();
    if (it_text.hasNext()){
      Hmm hmm = new Hmm();
      hmm.init(inkey.getRefName(),it_text);
      hmm.run();
      Iterator<String> it_res = hmm.getResults();
      outKey.set(inkey.getRefName());
      while(it_res.hasNext()){
        textRes.set(it_res.next());
        ctx.write(outKey,textRes);
      }
    }
    //Map<Integer,List<Float> > binMap = new HashMap();
  }
}
  
class AlleleDepthReducer
extends Reducer<RefPosBaseKey,DoubleWritable,
               RefPosBaseKey,DoubleWritable> {
  @Override protected void reduce(RefPosBaseKey inkey,Iterable<DoubleWritable> invals, Reducer<RefPosBaseKey,DoubleWritable, RefPosBaseKey,DoubleWritable>.Context ctx)
    throws InterruptedException, IOException{
      Iterator<DoubleWritable> it = invals.iterator();
      double sum = 0.;
      while(it.hasNext()){
        sum+= it.next().get();
      }      
      if(sum>=1.0) ctx.write(inkey,new DoubleWritable(sum));  
      //System.err.println("REDUCER: "+inkey.toString()+": "+sum);
    }
}


