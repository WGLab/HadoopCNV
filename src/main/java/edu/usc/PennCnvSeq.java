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

import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.AlignmentBlock;

import org.seqdoop.hadoop_bam.AnySAMInputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

public class PennCnvSeq
extends Configured{

  Configuration conf;
  Path bamfile;
  String bamfileStr;
  public static final int base_spacing = 1;
  public static final int bin_length = 1000;
  
  public PennCnvSeq(Configuration conf,String args[]){
    this.conf = conf;
    this.bamfileStr = args[2];
    this.bamfile = new Path(bamfileStr);
    System.err.println("Bamfile is at "+bamfile);
    if(conf==null) System.err.println("Configuration null!");
  }


  public void run(){
    try{
      int numReduceTasksBig = 800;
      int numReduceTasksSmall = 100;
      boolean runDepthCallJob = true;
      boolean runRegionBinJob = true;
      boolean runCnvCallJob = true;
      boolean runGlobalSortJob = false;
      if(runDepthCallJob){
        //conf.setInt("dfs.blocksize",512*1024*1024);
        Job depthCallJob = new Job(conf);
        depthCallJob.setJobName("depth_caller");
        depthCallJob.setNumReduceTasks(numReduceTasksBig);
        depthCallJob.setInputFormatClass(AnySAMInputFormat.class);
        depthCallJob.setJarByClass(PennCnvSeq.class);
        depthCallJob.setMapperClass(SAMRecordMapper.class);
        depthCallJob.setMapOutputKeyClass(RefPosBaseKey.class);
        //depthCallJob.setMapOutputKeyClass(LongWritable.class);
        depthCallJob.setMapOutputValueClass(DoubleWritable.class);
        depthCallJob.setCombinerClass(AlleleDepthReducer.class);
        depthCallJob.setReducerClass(AlleleDepthReducer.class);
        depthCallJob.setOutputKeyClass   (RefPosBaseKey.class);
        //depthCallJob.setOutputKeyClass   (NullWritable.class);
        depthCallJob.setOutputValueClass (DoubleWritable.class);
        //FileInputFormat.setInputPathFilter(depthCallJob,GlobFilter.class);
        FileInputFormat.addInputPath(depthCallJob,bamfile);
        FileOutputFormat.setOutputPath(depthCallJob,new Path("workdir/depth"));
        System.err.println("DEPTH CALL JOB submitting.");
        if (!depthCallJob.waitForCompletion(true)) {
          System.err.println("DEPTH CALL JOB failed.");
          return;
        }
      }
      if(runRegionBinJob){
        Job regionBinJob = new Job(conf);
        regionBinJob.setJobName("binner");
        regionBinJob.setJarByClass(PennCnvSeq.class);
        regionBinJob.setNumReduceTasks(numReduceTasksSmall);
        regionBinJob.setInputFormatClass(TextInputFormat.class);
        regionBinJob.setMapperClass(BinMapper.class);
        regionBinJob.setMapOutputKeyClass(RefBinKey.class);
        regionBinJob.setMapOutputValueClass(Text.class);
        regionBinJob.setReducerClass(BinReducer.class);
        regionBinJob.setOutputKeyClass(RefBinKey.class);
        regionBinJob.setOutputValueClass (Text.class);
  
        FileInputFormat.addInputPath(regionBinJob,new Path("workdir/depth/"));
        FileOutputFormat.setOutputPath(regionBinJob,new Path("workdir/bins/"));
        System.err.println("REGION BIN JOB submitting.");
        if (!regionBinJob.waitForCompletion(true)) {
          System.err.println("REGION BIN JOB failed.");
          return;
        }
      }
      if(runCnvCallJob){
        Job secondarySortJob = new Job(conf);
        secondarySortJob.setJobName("secondary_sorter");
        secondarySortJob.setJarByClass(PennCnvSeq.class);
        secondarySortJob.setNumReduceTasks(24);
        secondarySortJob.setInputFormatClass(TextInputFormat.class);
        secondarySortJob.setMapperClass(BinSortMapper.class);
        secondarySortJob.setMapOutputKeyClass(RefBinKey.class);
        secondarySortJob.setMapOutputValueClass(Text.class);
        secondarySortJob.setPartitionerClass(ChrPartitioner.class);
        secondarySortJob.setGroupingComparatorClass(ChrGroupingComparator.class);
        //secondarySortJob.setSortComparatorClass(RefPosKeyComparator.class);
        secondarySortJob.setReducerClass(CnvReducer.class);
        //secondarySortJob.setReducerClass(Reducer.class);
        secondarySortJob.setOutputKeyClass(Text.class);
        //secondarySortJob.setOutputKeyClass(RefBinKey.class);
        secondarySortJob.setOutputValueClass (Text.class);
        FileInputFormat.addInputPath(secondarySortJob,new Path("workdir/bins/"));
        FileOutputFormat.setOutputPath(secondarySortJob,new Path("workdir/cnv/"));
        System.err.println("SECONDARY SORT JOB submitting.");
        if (!secondarySortJob.waitForCompletion(true)) {
          System.err.println("SECONDARY SORT JOB failed.");
          return;
        }
      }
      if(runGlobalSortJob){
        int numReduceTasks = numReduceTasksSmall;
        double pcnt = 10.0;
        int numSamples = numReduceTasks;
        int maxSplits = numReduceTasks - 1;
        if (0 >= maxSplits)
          maxSplits = Integer.MAX_VALUE;
        InputSampler.Sampler sampler = new InputSampler.RandomSampler(pcnt, numSamples, maxSplits);
  
        Job regionSortJob = new Job(conf);
        regionSortJob.setJobName("sorter");
        regionSortJob.setJarByClass(PennCnvSeq.class);
        regionSortJob.setNumReduceTasks(numReduceTasks);
        regionSortJob.setInputFormatClass(SortInputFormat.class);
        regionSortJob.setMapperClass(Mapper.class);
        regionSortJob.setMapOutputKeyClass(RefPosBaseKey.class);
        regionSortJob.setMapOutputValueClass(Text.class);
        regionSortJob.setReducerClass(Reducer.class);
        regionSortJob.setOutputKeyClass(RefPosBaseKey.class);
        regionSortJob.setOutputValueClass (Text.class);
        FileInputFormat.addInputPath(regionSortJob,new Path("workdir/depth/"));
        FileOutputFormat.setOutputPath(regionSortJob,new Path("workdir/sorted/"));
        regionSortJob.setPartitionerClass(TotalOrderPartitioner.class);
        TotalOrderPartitioner.setPartitionFile(regionSortJob.getConfiguration(),new Path("workdir/partitioning"));
        InputSampler.writePartitionFile(regionSortJob, sampler);
        System.err.println("REGION SORT JOB submitting.");
        if (!regionSortJob.waitForCompletion(true)) {
          System.err.println("REGION SORT JOB failed.");
          return;
        }
      }
    }catch(IOException ex){
      ex.printStackTrace();
      System.exit(1);
    }catch(InterruptedException ex){
      ex.printStackTrace();
      System.exit(1);
    }catch(ClassNotFoundException ex){
      ex.printStackTrace();
      System.exit(1);
    }
  }

  public static void main(String[] args){
    try{
      GenericOptionsParser parser = new GenericOptionsParser(args);
      SAMRecord record;
      PennCnvSeq pennCnvSeq = new PennCnvSeq(parser.getConfiguration(),args);
      for(int i=0;i<args.length;++i) System.err.println("Argument "+i+": "+args[i]);
      pennCnvSeq.run();
      System.err.println("I'm done");
    }catch(Exception ex){
      ex.printStackTrace();
      System.exit(1);
    }
    
  }
}

class SAMRecordMapper
extends Mapper<LongWritable,SAMRecordWritable,
               RefPosBaseKey,DoubleWritable> {
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
          if(refpos % PennCnvSeq.base_spacing == 0){
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
        // Let's break up the string into chunks of 10
      }
    }
}



class AlleleDepthReducer
extends Reducer<RefPosBaseKey,DoubleWritable,
               RefPosBaseKey,DoubleWritable> {
  @Override protected void reduce(RefPosBaseKey inkey,Iterable<DoubleWritable> invals,
                               Reducer<RefPosBaseKey,DoubleWritable,
                               RefPosBaseKey,DoubleWritable>.Context ctx)
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
    //Map<Long,List<Float> > binMap = new HashMap();
  }
}

class BinMapper
extends Mapper<LongWritable,Text,
               RefBinKey,Text> {
  private final RefBinKey refBinKey = new RefBinKey();
  private final Text newVal = new Text();
  @Override protected void map(LongWritable inkey,Text inval,
                               Mapper<LongWritable,Text,
                               RefBinKey,Text>.Context ctx)
    throws InterruptedException, IOException{
      String []parts = inval.toString().split("\t");
      String refname = parts[0];
      int bin = (int)(Long.parseLong(parts[1])/PennCnvSeq.bin_length);
      String binMapValue = StringUtils.join(parts,"\t",1,4);
      //System.err.println("BinMap value: "+binMapValue);
      refBinKey.setRefName(refname);
      refBinKey.setBin(bin);
      newVal.set(binMapValue);
      ctx.write(refBinKey,newVal);  
     
    }
}

class BinReducer
extends Reducer<RefBinKey,Text,
               RefBinKey,Text> {

  private final Text retText = new Text();
  private final Map<Long,List<Float> > binMap = new HashMap();
  private final Set<Float> depthSet = new TreeSet();
  private final Set<Float> bafSet = new TreeSet();

  @Override 
  protected void reduce(RefBinKey inkey,Iterable<Text> invals,
  Reducer<RefBinKey,Text, RefBinKey,Text>.Context ctx)
  throws InterruptedException, IOException{
    Iterator<Text> it = invals.iterator();
    binMap.clear();
    long startbp = Long.MAX_VALUE;
    long endbp = Long.MIN_VALUE;
    while(it.hasNext()){
      Text text = it.next();
      String[] parts = text.toString().split("\t");
      int p=0;
      Long bp = Long.decode(parts[p++]);
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
    Iterator<Long> it_keys = binMap.keySet().iterator();
    depthSet.clear();
    bafSet.clear();
    double total_depth = 0;
    int n=0;
    int hets = 0;
    while(it_keys.hasNext()){
      Long bp = it_keys.next();
      Iterator<Float> depthListIterator = binMap.get(bp).iterator();
      float maxDepth = 0,pos_depth=0;
      while(depthListIterator.hasNext()){
        float depth = depthListIterator.next().floatValue();
        pos_depth+=depth;
        if(depth>maxDepth) maxDepth = depth;
      }
      float baf = (pos_depth-maxDepth)/pos_depth;
      //if(pos_depth<20 || baf<0.1) baf = 0f;
      if(baf > 0f) ++hets;
      //System.err.println("BIN REDUCER adding at "+bp+" : "+pos_depth+","+baf);
      depthSet.add(pos_depth);
      total_depth+=pos_depth;
      ++n;
      bafSet.add(baf);
    }
    double mean_depth=total_depth/n;
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
    retText.set(Long.toString(startbp)+"\t"+Long.toString(endbp)+"\t"+Double.toString(medianDepth)+"\t"+Integer.toString(hets)+"\t"+Double.toString(total_depth)+"\t"+Integer.toString(n));
    ctx.write(inkey, retText);
  }
}

/*
 *  KEY CLASSES BEGIN HERE
 *
 */

class RefRangeAlignmentKey implements
WritableComparable<RefRangeAlignmentKey>{

  private String refname;
  private long position1;
  private long position2;
  private String bases;

  public RefRangeAlignmentKey(){}

  public RefRangeAlignmentKey(String refname,long position1,long position2, String bases){
    this.refname = refname;
    this.position1 = position1;
    this.position2 = position2;
    this.bases = bases;
  }

  public void setRefName(String refname){
    this.refname = refname;
  }
  
  public void setPosition1(long position1){
    this.position1 = position1;
  }

  public void setPosition2(long position2){
    this.position2 = position2;
  }

  public void setBase(String bases){
    this.bases = bases;
  }

  public void write(DataOutput out) throws IOException {
    out.writeUTF(refname);
    out.writeLong(position1);
    out.writeLong(position2);
    out.writeUTF(bases);
  }

  public void readFields(DataInput in) throws IOException {
    refname = in.readUTF();
    position1 = in.readLong();
    position2 = in.readLong();
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
    return refname+"\t"+Long.toString(position1)+"\t"+Long.toString(position2)+"\t"+bases;
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
  private long position;
  private int base;

  public RefPosBaseKey(){}
  public RefPosBaseKey(String refname,long position, int base){
    this.refname = refname;
    this.position = position;
    this.base = base;
  }

  public void setRefName(String refname){
    this.refname = refname;
  }
  
  public void setPosition(long position){
    this.position = position;
  }

  public void setBase(int base){
    this.base = base;
  }

  public void write(DataOutput out) throws IOException {
    out.writeUTF(refname);
    out.writeLong(position);
    out.writeInt(base);
  }

  public void readFields(DataInput in) throws IOException {
    refname = in.readUTF();
    position = in.readLong();
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
    return refname+"\t"+Long.toString(position)+"\t"+Integer.toString(base);
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

/*
 *  KEY CLASSES END HERE
 *
 */

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
    //long binRange = (long)(Long.parseLong(parts[1]) / bin_length);
    //System.err.println("BINRANGE for "+parts[1]+" is "+binRange);
    //RefPosBaseKey newKey = new RefPosBaseKey(parts[0],binRange);
    //RefPosBaseKey newKey = new RefPosBaseKey(parts[0],Long.parseLong(parts[1]));
    currentKey.setRefName(parts[0]);
    currentKey.setPosition(Long.parseLong(parts[1]));
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

class ChrPartitioner extends Partitioner<RefBinKey,Text>{
  /*
 * Delegates the refname of the composite key to the hash partitioner
 */
  private final HashPartitioner<Text,Text> hashPartitioner = new HashPartitioner<Text,Text>();
  private final Text newKey = new Text();

  @Override
  public int getPartition(RefBinKey key,Text value, int numReduceTasks){
    try{
      newKey.set(key.getRefName());
      return hashPartitioner.getPartition(newKey,value,numReduceTasks);
    }catch (Exception ex){
      ex.printStackTrace();
      return (int)(Math.random()*numReduceTasks);
    }
  }
}

class ChrGroupingComparator extends WritableComparator{
  protected ChrGroupingComparator(){
    super(RefBinKey.class,true);
  }
  @Override
  public int compare(WritableComparable w1, WritableComparable w2) {
    RefBinKey key1 = (RefBinKey)w1;
    RefBinKey key2 = (RefBinKey)w2;
    return key1.getRefName().compareTo(key2.getRefName());
  }
}
