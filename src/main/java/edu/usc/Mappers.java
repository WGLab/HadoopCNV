package edu.usc;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.AlignmentBlock;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

public class Mappers{
  // we can put any common utilities in here
}

class BinMapper
extends Mapper<LongWritable,Text,
               RefBinKey,Text> {

  // this is the hard-coded length of the bin size

  private final RefBinKey refBinKey = new RefBinKey();


  private final Text newVal = new Text();
  @Override protected void map(LongWritable inkey,Text inval,
                               Mapper<LongWritable,Text,
                               RefBinKey,Text>.Context ctx)
    throws InterruptedException, IOException{
      String []parts = inval.toString().split("\t");
      String refname = parts[0];
      //System.err.println("Bin length value: "+Constants.bin_width);
      int bin = (int)(Integer.parseInt(parts[1])/Constants.bin_width);
      String binMapValue = StringUtils.join(parts,"\t",1,4);
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
