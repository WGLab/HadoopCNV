package edu.usc;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.LongWritable;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.AlignmentBlock;
import org.seqdoop.hadoop_bam.SAMRecordWritable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Placeholder class for Mappers. Currently no functionality here.
 *
 * @author Gary Chen, Ph.D.
 */
public class Mappers {
}

/**
 * This is the Mapper for grouping output from the Depth Caller into bins. It
 * specifically assigns a bin ID for all bases that map to a bin.
 */
class BinMapper
        extends Mapper<LongWritable, Text, RefBinKey, Text> {

    private final RefBinKey refBinKey = new RefBinKey();
    private final Text newVal = new Text();

    /**
     * @param inkey A sequence ID for the text input
     * @param inval A text record consisting of Chr, base pair, allele, and
     * depth
     * @param ctx a handle to the Mapper context
     */
    @Override
    protected void map(LongWritable inkey, Text inval,
            Mapper<LongWritable, Text, RefBinKey, Text>.Context ctx)
            throws InterruptedException, IOException {
        boolean debug = false;
        String[] parts = inval.toString().split("\t");
        String refname = parts[0];
        if (debug) {
            System.err.println("Bin length value: " + Constants.bin_width);
        }
        int bp = Integer.parseInt(parts[1]);
        int bin = (int) (bp / Constants.bin_width) * Constants.bin_width;
        String binMapValue = StringUtils.join(parts, "\t", 1, 4);
        refBinKey.setRefName(refname);
        refBinKey.setBin(bin);
        newVal.set(binMapValue);
        if (debug) {
            System.err.println("Emitting:" + refBinKey.toString() + ":" + newVal.toString());
        }
        ctx.write(refBinKey, newVal);
    }
}

class BinSortMapper
        extends Mapper<LongWritable, Text, RefBinKey, Text> {

    private final Text newVal = new Text();

    @Override
    protected void map(LongWritable inkey, Text inval,
            Mapper<LongWritable, Text, RefBinKey, Text>.Context ctx)
            throws InterruptedException, IOException {
        String[] parts = inval.toString().split("\t");
        String refname = parts[0];
        int bin = Integer.parseInt(parts[1]);
        String value = StringUtils.join(parts, "\t", 2, parts.length);
        //System.err.println("BinMap value: "+binMapValue);
        newVal.set(value);
        ctx.write(new RefBinKey(refname, bin), newVal);
    }
}

/**
 * This is the mapper to retrieve all the split reads
 *
 * @see Constants This version is considerably faster than the naive version of
 * the Depth Caller.
 */
class SRPEReadMapper extends Mapper<LongWritable, SAMRecordWritable, RefBinKey, Text> {

    private RefBinKey refBinKey = null;
    private Text text = null;

    public SRPEReadMapper() {
        refBinKey = new RefBinKey();
        text = new Text();
    }
    
    @Override
    protected void map(LongWritable inkey, SAMRecordWritable inval,
            Mapper<LongWritable, SAMRecordWritable, RefBinKey, Text>.Context ctx)
            throws InterruptedException, IOException {
                SAMRecord samrecord = inval.get();
                String refname = samrecord.getReferenceName();
                Pattern p = Pattern.compile("^chr");
                Matcher m = p.matcher(refname);
                if(!m.lookingAt()) {
                	refname = "chr" + refname;
                }
                refBinKey.setRefName(refname);
                String samstr = samrecord.getSAMString();
                int start = samrecord.getAlignmentStart();
                refBinKey.setBin(start);
                String SA = samrecord.getStringAttribute("SA");
                int flag = samrecord.getFlags();
                if ( (flag & 1294) != 0) {
                	ctx.write(refBinKey, new Text("PE\t" + samstr) );
                }
                if (SA != null){
                	ctx.write(refBinKey, new Text("SR\t" + samstr) );
                }
                return;
        
    }
}


/**
 * This is the Mapper of the Depth Caller that uses a fixed length view of the
 * reads that overlap within the range of the view. To set the view width
 * configure this value in the Constants.java
 *
 * @see Constants This version is considerably faster than the naive version of
 * the Depth Caller.
 */
class SAMRecordWindowMapper extends Mapper<LongWritable, SAMRecordWritable, RefBinKey, ArrayPrimitiveWritable> {

    private RefBinKey refBinKey = null;
    private ArrayPrimitiveWritable arrayPrimitiveWritable = null;
    private final int bin_size = Constants.read_bin_width;
    private byte[] read_info = null;
    
    public SAMRecordWindowMapper() {
        refBinKey = new RefBinKey();
        arrayPrimitiveWritable = new ArrayPrimitiveWritable();
        read_info = new byte[bin_size * 2];
    }

    /**
     * Input splits from a BAM file are provided by the Hadoop-BAM library.
     * These come in the form of SAMRecord objects, which have functions for
     * extracting properties of each read. What is output by this mapper is a
     * key-value pair where the key is composite key consisiting of the
     * Chromosome and the first base pair coordinate of the view. The value is a
     * 2N length byte array (N being the width of the view), where elements
     * 0,2,4,...,N-2 store the 1 byte integer value of the allele at positions
     * 1,2,3,...,N-1, and elements 1,3,5,...,N-1 store the 1-byte integer value
     * of the phred quality scores of the allele in the left adjacent element.
     *
     * @param inkey A long integer which is basically a sequence in the BAM file
     * @param inval A single SAM record
     * @param ctx The mapper context. Used as a handle to write output to the
     * Hadoop framework.
     */
    @Override
    protected void map(LongWritable inkey, SAMRecordWritable inval,
            Mapper<LongWritable, SAMRecordWritable, RefBinKey, ArrayPrimitiveWritable>.Context ctx)
            throws InterruptedException, IOException {
        boolean debug = false;

        // use the bin size to output fragments of reads in 
        // regular intervals
        SAMRecord samrecord = inval.get();
        String refname = samrecord.getReferenceName();
        Pattern p = Pattern.compile("^chr");
        Matcher m = p.matcher(refname);
        if(!m.lookingAt()) {
        	refname = "chr" + refname;
        }
        
        refBinKey.setRefName(refname);
        String samstr = samrecord.getSAMString();

        if (debug) {
            System.err.println("SAMSTRING: " + samstr);
            System.err.println(" readlen: " + samrecord.getReadLength());
        }

        byte[] bases = samrecord.getReadBases();
        byte[] basequals = samrecord.getBaseQualities();
        int mapqual = samrecord.getMappingQuality();
        double mapprob = 1. - Math.pow(10, -mapqual * .1);
       
        //System.err.println(" mapquality "+mapprob);
        //System.err.println(" bytelen: "+bases.length);
        //System.err.println(" first base: "+Byte.toString(bases[0]));
        //System.err.println(" cigar: "+samrecord.getCigar().toString());
        //System.err.println(" quality: "+samrecord.getMappingQuality());
        if (mapprob >= Constants.mapping_quality_threshold) {
            List<AlignmentBlock> alignmentBlockList = samrecord.getAlignmentBlocks();
            Iterator<AlignmentBlock> it = alignmentBlockList.iterator();
            for (int j = 0; j < bin_size; ++j) {
                read_info[j * 2] = 0;
                read_info[j * 2 + 1] = 0;
            }
            while (it.hasNext()) {
                AlignmentBlock alignmentBlock = it.next();
                int refstart = alignmentBlock.getReferenceStart();
                int readstart = alignmentBlock.getReadStart();
                int readstart0 = readstart - 1;
                int alignlen = alignmentBlock.getLength();
                int last_bin = -1;
                int current_bin;    // = 0;
                int refpos_1;       // = 1;
                int refpos_0 = 0;
                for (int i = 0; i < alignlen; ++i) {
                    refpos_1 = refstart + i;
                    refpos_0 = refpos_1 - 1;
                    if (debug) {
                        System.err.println(" ref_start " + refstart + " readstart " + readstart + " refpos_0:" + refpos_0);
                    }
                    // the current bin
                    current_bin = (int) (refpos_0 / bin_size) * bin_size;
                    //int current_refpos = refpos_0 % bin_size;
                    if (current_bin != last_bin) {
                        if (last_bin != -1) {
                            // we have already populated at least one bin so output it
                            // Remember that this current bin is zero-based indexed!
                            refBinKey.setBin(last_bin);
                            arrayPrimitiveWritable.set(read_info);
                            ctx.write(refBinKey, arrayPrimitiveWritable);
                            if (debug) {
                                System.err.println(" Emitting " + refBinKey.toString() + " currentpos " + refpos_0);
                            }
                        }
                        for (int j = 0; j < bin_size; ++j) {
                            read_info[j * 2] = 0;
                            read_info[j * 2 + 1] = 0;
                        }
                    }
                    if (true) {
                        int read_pos_index = readstart0 + i;
   
                        if (debug) {
                            System.err.println(" Storing " + (int) bases[read_pos_index] + " of position " + read_pos_index + " into " + ((refpos_0) % bin_size) * 2);
                        }
                        if(read_pos_index < bases.length)
                        {
                        	read_info[((refpos_0) % bin_size) * 2] = bases[read_pos_index];
                        	read_info[((refpos_0) % bin_size) * 2 + 1] = basequals[read_pos_index];
                        }
                        else{
                        	read_info[((refpos_0) % bin_size) * 2] = (byte) 'A';
                        	read_info[((refpos_0) % bin_size) * 2 + 1] = 1;
                        }
                    }
                    last_bin = current_bin;  
                }
                if (last_bin != -1) { 
                    // we have already populated at least one bin so output it
                    // Remember that this current bin is zero-based indexed!
                    refBinKey.setBin(last_bin);
                    arrayPrimitiveWritable.set(read_info);
                    if (debug) {
                        System.err.println(" Last emitting " + refBinKey.toString() + " current " + refpos_0);
                    }
                    ctx.write(refBinKey, arrayPrimitiveWritable);
                }
            }
        }
    }
}
