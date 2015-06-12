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

/**
 * Placeholder class for Mappers. Currently no functionality here.
 */

public class Mappers{
}

/**
 * This is the Mapper for grouping output from the Depth Caller
 * into bins. It specifically assigns a bin ID for all bases that map
 * to a bin.
 */
class BinMapper
extends Mapper<LongWritable,Text,
               RefBinKey,Text> {


  private final RefBinKey refBinKey = new RefBinKey();
  private final Text newVal = new Text();

/**
 * @param inkey A sequence ID for the text input
 * @param inval A text record consisting of Chr, base pair, allele, and depth
 * @param ctx a handle to the Mapper context
 */

  @Override protected void map(LongWritable inkey,Text inval,
                               Mapper<LongWritable,Text,
                               RefBinKey,Text>.Context ctx)
    throws InterruptedException, IOException{
      boolean debug = false;
      String []parts = inval.toString().split("\t");
      String refname = parts[0];
      if(debug)System.err.println("Bin length value: "+Constants.bin_width);
      int bp = Integer.parseInt(parts[1]);
      int bin = (int)(bp/Constants.bin_width) * Constants.bin_width;
      String binMapValue = StringUtils.join(parts,"\t",1,4);
      refBinKey.setRefName(refname);
      refBinKey.setBin(bin);
      newVal.set(binMapValue);
      if(debug)System.err.println("Emitting:"+refBinKey.toString()+":"+newVal.toString());
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

///**
// * This is the Mapper of the naive Depth Caller that computes an expected
// * depth based on the 0-1 range value of the quality score, converted from
// * a phred score
// * This version is considerably slower than SAMRecordWindowMapper
// * Caller.
// */
//
//class SAMRecordMapper
//extends Mapper<LongWritable,SAMRecordWritable,
//               RefPosBaseKey,DoubleWritable> {
//
//  //VcfLookup lookup = null;
//  private RefPosBaseKey refPosBaseKey = null;
//  private DoubleWritable doubleWritable = null;
//  public SAMRecordMapper(){
//    //lookup = new VcfLookup();
//    //lookup.readObject();
//    refPosBaseKey = new RefPosBaseKey();
//    doubleWritable = new DoubleWritable();
//  }
//
///**
// * Input splits from a BAM file are provided by the Hadoop-BAM library.
// * These come in the form of SAMRecord objects, which have functions for 
// * extracting properties of each read. What is output by this mapper
// * is a key-value pair where the key is composite key consisiting of the
// * Chromosome, the base pair position, and the allele (integer). The value is
// * a floating point value of the quality score, converted by the phred score
// * @param inkey A long integer which is basically a sequence in the BAM file
// * @param inval A single SAM record
// * @param ctx The mapper context. Used as a handle to write output to the 
// * Hadoop framework.
// */
//
//  @Override protected void map(LongWritable inkey,SAMRecordWritable inval,
//                               Mapper<LongWritable,SAMRecordWritable,
//                               RefPosBaseKey,DoubleWritable>.Context ctx)
//    throws InterruptedException, IOException{
//      SAMRecord samrecord = inval.get();    
//      String refname = samrecord.getReferenceName();
//      refPosBaseKey.setRefName(refname);
//      //String samstr = samrecord.getSAMString();
//      //System.err.println("SAMSTRING: "+samstr);
//      //System.err.println(" readlen: "+samrecord.getReadLength());
//      byte[] bases = samrecord.getReadBases();
//      byte[] basequals = samrecord.getBaseQualities();
//      int mapqual = samrecord.getMappingQuality();
//      double mapprob = 1.-Math.pow(10,-mapqual*.1);
//      if(mapprob>Constants.mapping_quality_threshold){
//        //System.err.println(" mapquality "+mapprob);
//        //System.err.println(" bytelen: "+bases.length);
//        //System.err.println(" first base: "+Byte.toString(bases[0]));
//        //System.err.println(" cigar: "+samrecord.getCigar().toString());
//        //System.err.println(" quality: "+samrecord.getMappingQuality());
//        List<AlignmentBlock> alignmentBlockList = samrecord.getAlignmentBlocks();
//        Iterator<AlignmentBlock> it = alignmentBlockList.iterator();
//        while(it.hasNext()){
//          AlignmentBlock alignmentBlock = it.next();
//          //System.err.println("  block start "+alignmentBlock.getReferenceStart()+"("+alignmentBlock.getReadStart()+") with length "+alignmentBlock.getLength());
//          int refstart = alignmentBlock.getReferenceStart();
//          int readstart = alignmentBlock.getReadStart();
//          int alignlen = alignmentBlock.getLength();
//          //char []basechars = new char[alignlen];
//          for(int i=0;i<alignlen;++i){
//            int refpos = refstart + i;
//            if(true){
//            //if(lookup.exists(refname,refpos)){
//            // Let's break up the string into chunks of 10
//            //if(refpos % PennCnvSeq.base_spacing == 0){
//              int base = (int)bases[readstart+i-1];
//              //basechars[i] = (char)base;
//              int basequal = (int)basequals[readstart+i-1];
//              double baseprob = 1.-Math.pow(10,-basequal*.1);
//              //System.err.println(" base "+base+" quality: "+(1.-Math.pow(10,-basequal*.1)));
//              double fullprob = mapprob;
//              //double fullprob = mapprob*baseprob;
//              if(baseprob>.99) {
//                refPosBaseKey.setPosition(refstart+i);
//                refPosBaseKey.setBase(base);
//                doubleWritable.set(fullprob);
//                ctx.write(refPosBaseKey,doubleWritable);
//              }
//            }
//          }
//        }
//      }
//    }
//}


/**
 * This is the Mapper of the Depth Caller that uses a fixed length view of 
 * the reads that overlap within the range of the view. To set the view width
 * configure this value in the Constants.java
 * @see Constants
 * This version is considerably faster than the naive version of the Depth
 * Caller.
 */

class SAMRecordWindowMapper
extends Mapper<LongWritable,SAMRecordWritable,
               RefBinKey,ArrayPrimitiveWritable> {
  private RefBinKey refBinKey = null; 
  private ArrayPrimitiveWritable arrayPrimitiveWritable = null; 
  private int bin_size = Constants.read_bin_width;
  private byte[] read_info = null;

  public SAMRecordWindowMapper(){
    refBinKey = new RefBinKey();
    arrayPrimitiveWritable = new ArrayPrimitiveWritable();
    read_info = new byte[bin_size*2];
  }

/**
 * Input splits from a BAM file are provided by the Hadoop-BAM library.
 * These come in the form of SAMRecord objects, which have functions for 
 * extracting properties of each read. What is output by this mapper
 * is a key-value pair where the key is composite key consisiting of the
 * Chromosome and the first base pair coordinate of the view. The value is
 * a 2N length byte array (N being the width of the view), where elements
 * 0,2,4,...,N-2 store the 1 byte integer value of the allele at positions 
 * 1,2,3,...,N-1, and elements 1,3,5,...,N-1 store the 1-byte integer value
 * of the phred quality scores of the allele in the left adjacent element.
 * @param inkey A long integer which is basically a sequence in the BAM file
 * @param inval A single SAM record
 * @param ctx The mapper context. Used as a handle to write output to the 
 * Hadoop framework.
 */

  @Override protected void map(LongWritable inkey,SAMRecordWritable inval,
                               Mapper<LongWritable,SAMRecordWritable,
                               RefBinKey,ArrayPrimitiveWritable>.Context ctx)
    throws InterruptedException, IOException{
      boolean debug = false;
 
      // use the bin size to output fragments of reads in 
      // regular intervals
      int bin_size = Constants.read_bin_width;
      SAMRecord samrecord = inval.get();    
      String refname = samrecord.getReferenceName();
      refBinKey.setRefName(refname);
      String samstr = samrecord.getSAMString();
      if(debug)System.err.println("SAMSTRING: "+samstr);
      if(debug)System.err.println(" readlen: "+samrecord.getReadLength());
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
        for(int j=0;j<bin_size;++j){
          read_info[j*2] = 0;
          read_info[j*2+1] = 0;
        }
        while(it.hasNext()){
          AlignmentBlock alignmentBlock = it.next();
          int refstart = alignmentBlock.getReferenceStart();
          int readstart = alignmentBlock.getReadStart();
          int readstart0 = readstart-1;
          int alignlen = alignmentBlock.getLength();
          int last_bin = -1;
          int current_bin = 0;
          int refpos_1 = 1;
          int refpos_0 = 0;
          for(int i=0;i<alignlen;++i){
            refpos_1 = refstart + i;
            refpos_0 = refpos_1-1;
            if(debug)System.err.println(" ref_start "+refstart+" readstart "+readstart+" refpos_0:"+refpos_0);
            // the current bin
            current_bin = (int)(refpos_0/bin_size)*bin_size;
            //int current_refpos = refpos_0 % bin_size;
            if(current_bin!=last_bin){
              if(last_bin!=-1){
                // we have already populated at least one bin so output it
                // Remember that this current bin is zero-based indexed!
                refBinKey.setBin(last_bin);
                arrayPrimitiveWritable.set(read_info);
                ctx.write(refBinKey,arrayPrimitiveWritable);  
                if(debug)System.err.println(" Emitting "+refBinKey.toString()+" currentpos "+refpos_0);
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
              if(debug)System.err.println(" Storing "+(int)bases[read_pos_index]+" of position "+read_pos_index+" into "+((refpos_0)%bin_size)* 2);
              read_info[((refpos_0)%bin_size)* 2] = bases[read_pos_index];
              read_info[((refpos_0)%bin_size)* 2+1] = basequals[read_pos_index];
            }
            last_bin = current_bin;
          }
          if(last_bin!=-1){
            // we have already populated at least one bin so output it
            // Remember that this current bin is zero-based indexed!
            refBinKey.setBin(last_bin);
            arrayPrimitiveWritable.set(read_info);
            if(debug)System.err.println(" Last emitting "+refBinKey.toString()+" current "+refpos_0);
            ctx.write(refBinKey,arrayPrimitiveWritable);  
          }
        }
      }
    }
}
