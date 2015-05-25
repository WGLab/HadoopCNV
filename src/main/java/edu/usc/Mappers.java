package edu.usc;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
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

  //VcfLookup lookup = null;
  RefPosBaseKey refPosBaseKey = null;
  DoubleWritable doubleWritable = null;
  public SAMRecordMapper(){
    //lookup = new VcfLookup();
    //lookup.readObject();
    refPosBaseKey = new RefPosBaseKey();
    doubleWritable = new DoubleWritable();
  }

  @Override protected void map(LongWritable inkey,SAMRecordWritable inval,
                               Mapper<LongWritable,SAMRecordWritable,
                               RefPosBaseKey,DoubleWritable>.Context ctx)
    throws InterruptedException, IOException{
      SAMRecord samrecord = inval.get();    
      String refname = samrecord.getReferenceName();
      refPosBaseKey.setRefName(refname);
      //String samstr = samrecord.getSAMString();
      //System.err.println("SAMSTRING: "+samstr);
      //System.err.println(" readlen: "+samrecord.getReadLength());
      byte[] bases = samrecord.getReadBases();
      byte[] basequals = samrecord.getBaseQualities();
      int mapqual = samrecord.getMappingQuality();
      double mapprob = 1.-Math.pow(10,-mapqual*.1);
      if(mapprob>Constants.mapping_quality_threshold){
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
            if(true){
            //if(lookup.exists(refname,refpos)){
            // Let's break up the string into chunks of 10
            //if(refpos % PennCnvSeq.base_spacing == 0){
              int base = (int)bases[readstart+i-1];
              //basechars[i] = (char)base;
              int basequal = (int)basequals[readstart+i-1];
              double baseprob = 1.-Math.pow(10,-basequal*.1);
              //System.err.println(" base "+base+" quality: "+(1.-Math.pow(10,-basequal*.1)));
              double fullprob = mapprob;
              //double fullprob = mapprob*baseprob;
              if(baseprob>.99) {
                refPosBaseKey.setPosition(refstart+i);
                refPosBaseKey.setBase(base);
                doubleWritable.set(fullprob);
                ctx.write(refPosBaseKey,doubleWritable);
              }
            }
          }
        }
      }
    }
}


// this version uses an array that roughly is the size of reads with
// the hope that there are less maps and reduces to do
//
class SAMRecordWindowMapper
extends Mapper<LongWritable,SAMRecordWritable,
               RefBinKey,ArrayPrimitiveWritable> {

  // VcfLookup is an in-memory HashMap of variants to filter for
  // VcfLookup lookup = null;
  public SAMRecordWindowMapper(){
    //lookup = new VcfLookup();
    //lookup.readObject();
  }

  @Override protected void map(LongWritable inkey,SAMRecordWritable inval,
                               Mapper<LongWritable,SAMRecordWritable,
                               RefBinKey,ArrayPrimitiveWritable>.Context ctx)
    throws InterruptedException, IOException{
      RefBinKey refBinKey = new RefBinKey();
      ArrayPrimitiveWritable arrayPrimitiveWritable = 
      new ArrayPrimitiveWritable();
 
      // use the bin size to output fragments of reads in 
      // regular intervals
      int bin_size = Constants.read_bin_width;
      SAMRecord samrecord = inval.get();    
      String refname = samrecord.getReferenceName();
      refBinKey.setRefName(refname);
      String samstr = samrecord.getSAMString();
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
      if(mapprob>Constants.mapping_quality_threshold){
        List<AlignmentBlock> alignmentBlockList = samrecord.getAlignmentBlocks();
        Iterator<AlignmentBlock> it = alignmentBlockList.iterator();
        byte[] read_info = new byte[bin_size*2];
        for(int j=0;j<bin_size;++j){
          read_info[j*2] = 0;
          read_info[j*2+1] = 0;
        }
        while(it.hasNext()){
          AlignmentBlock alignmentBlock = it.next();
          //System.err.println("  block start "+alignmentBlock.getReferenceStart()+"("+alignmentBlock.getReadStart()+") with length "+alignmentBlock.getLength());
          int refstart = alignmentBlock.getReferenceStart();
          int readstart = alignmentBlock.getReadStart();
          int readstart0 = readstart-1;
          int alignlen = alignmentBlock.getLength();
          //char []basechars = new char[alignlen];
          int last_bin = -1,current_bin = 0;
          for(int i=0;i<alignlen;++i){
            int refpos_1 = refstart + i;
            int refpos_0 = readstart0 + refpos_1-1;
            // the current bin
            current_bin = (int)(refpos_0/bin_size)*bin_size;
            //int current_refpos = refpos_0 % bin_size;
            if(current_bin!=last_bin){
              if(last_bin!=-1){
                // we have already populated at least one bin so output it
                // Remember that this current bin is zero-based indexed!
                refBinKey.setBin(current_bin);
                arrayPrimitiveWritable.set(read_info);
                ctx.write(refBinKey,arrayPrimitiveWritable);  
                //System.err.println("Emitting");
              }
              for(int j=0;j<bin_size;++j){
                read_info[j*2] = 0;
                read_info[j*2+1] = 0;
              }
            }
            if(true){
            //if(lookup.exists(refname,refpos)){
              int read_pos_index = readstart0+i;
              int base = (int)bases[read_pos_index];
              //basechars[i] = (char)base;
              int basequal = (int)basequals[read_pos_index];
              //System.err.println("Last "+last_bin+" Current bin "+current_bin+" Storing "+(int)bases[read_pos_index]+" of position "+read_pos_index+" into "+((refpos_0)%bin_size)* 2);
              read_info[((refpos_0)%bin_size)* 2] = bases[read_pos_index];
              read_info[((refpos_0)%bin_size)* 2+1] = basequals[read_pos_index];
              //double baseprob = 1.-Math.pow(10,-basequal*.1);
              //System.err.println(" base "+base+" quality: "+(1.-Math.pow(10,-basequal*.1)));
              //double fullprob = mapprob;
              //double fullprob = mapprob*baseprob;
              //if(fullprob>.99) {
                //ctx.write(new RefPosBaseKey(refname,refstart+i,base),new DoubleWritable(fullprob));  
              //}
            }
            last_bin = current_bin;
          }
          if(last_bin!=-1){
            // we have already populated at least one bin so output it
            // Remember that this current bin is zero-based indexed!
            refBinKey.setBin(current_bin);
            arrayPrimitiveWritable.set(read_info);
            ctx.write(refBinKey,arrayPrimitiveWritable);  
            //System.err.println("Emitting last");
          }
        }
      }
    }
}
