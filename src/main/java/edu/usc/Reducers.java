package edu.usc;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.ArrayPrimitiveWritable;

/**
 * Placeholder class for the Reducers. Currently no functionality here.
 */

public class Reducers{
}

/**
 * Combines signal information across all bases within a bin into a single
 * data point for input into the HMM.
 */

class BinReducer
extends Reducer<RefBinKey,Text,
               RefBinKey,Text> {
  
  private final Text retText = new Text();
  private final Map<Integer,Boolean > binMapInVcf = new HashMap();
  private final Map<Integer,List<Float> > binMapDepth = new HashMap();
  private final Set<Float> depthSet = new TreeSet();
  private final Set<Float> bafSet = new TreeSet();
  private double[] mse_states  = new double[4];

/**
 * @param inkey A composite key consisting of Chromosome and Bin ID (first base
 * pair position of the bin)
 * @param invals A list of string 3-tuples where the first element is the 
 * the base pair position, the second element is the allele, and the third
 * element is the depth
 * @param ctx A handle to the reducer context so we can emit output
 */
  @Override 
  protected void reduce(RefBinKey inkey,Iterable<Text> invals,
  Reducer<RefBinKey,Text, RefBinKey,Text>.Context ctx)
  throws InterruptedException, IOException{
    boolean debug = false;
    Iterator<Text> it = invals.iterator();
    binMapInVcf.clear();
    binMapDepth.clear();
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
      boolean in_vcf = allele==0;
      if (binMapInVcf.get(bp)==null){
        binMapInVcf.put(bp,in_vcf);
      }else{
        if (in_vcf) binMapInVcf.put(bp,in_vcf);
      }
      Float depth = Float.valueOf(parts[p++]);
      if (binMapDepth.get(bp)==null){
        binMapDepth.put(bp,new java.util.ArrayList());
      }
      binMapDepth.get(bp).add(depth);
      //System.err.println("BIN REDUCER: "+inkey.toString()+" : "+bp+" : "+depth);
    }
    Iterator<Integer> it_keys = binMapDepth.keySet().iterator();
    depthSet.clear();
    bafSet.clear();
    double total_depth = 0;
    int n=0;
    while(it_keys.hasNext()){
      Integer bp = it_keys.next();
      Iterator<Float> depthListIterator = binMapDepth.get(bp).iterator();
      float maxDepth = 0,pos_depth=0;
      while(depthListIterator.hasNext()){
        float depth = depthListIterator.next().floatValue();
        pos_depth+=depth;
        if(depth>maxDepth) maxDepth = depth;
      }
      boolean intersect_vcf = binMapInVcf.get(bp) && pos_depth>0;
      //if(pos_depth<20 || baf<0.1) baf = 0f;
      if(intersect_vcf){ 
        float baf = (pos_depth-maxDepth)/pos_depth;
        bafSet.add(baf);
      }
      depthSet.add(pos_depth);
      total_depth+=pos_depth;
      ++n;
    }
    // we should always add a dummy BAF in case no VCF entries in this bin
    if(bafSet.size()==0) bafSet.add(0f);
    int baf_n = bafSet.size();
    double mean_depth=(n>0)?total_depth/n:0;
    Iterator<Float> depthSetIt = depthSet.iterator();
    Iterator<Float> bafSetIt = bafSet.iterator();
    float medianDepth = 0;
    float medianBaf = 0;
    for(int i=0;i<depthSet.size()/2;++i){
      medianDepth = depthSetIt.next();
    }
    //state 0 = CN=1
    //state 1 = CN=2 LOH
    //state 2 = CN=2 Normal
    //state 3 = CN=3 
    for(int i=0;i<4;++i) mse_states[i] = 0;
    int currentIter=0;
    int exp_het_index = (int)((1f-Constants.min_heterozygosity) * baf_n);
    while(bafSetIt.hasNext()){
      float currentBaf = bafSetIt.next(); 
      if(currentIter==(baf_n/2)){
        medianBaf = currentBaf;
      }
      float currentBaf2 = currentBaf*currentBaf;
      mse_states[0]+= currentBaf2;
      mse_states[1]+= currentBaf2;
      mse_states[2]+= currentIter>=exp_het_index?(.5-currentBaf)*(.5-currentBaf) : currentBaf2;
      mse_states[3]+= currentIter>=exp_het_index?(.33-currentBaf)*(.33-currentBaf) : currentBaf2;
      if(debug)System.err.println("DEBUG: currentIter: "+currentIter+" exp_het_index: "+exp_het_index);
      //mse_states[2]+= Math.min(currentBaf2, (.5-currentBaf)*(.5-currentBaf));
      //mse_states[3]+= Math.min(currentBaf2, (.33-currentBaf)*(.33-currentBaf));
      ++currentIter;
    }
    if(baf_n>0){
      for(int i=0;i<4;++i) mse_states[i]/= baf_n;
    }
    String bafVecStr = mse_states[0]+"\t"+mse_states[1]+"\t"+mse_states[2]+"\t"+mse_states[3];
    //System.err.println("BIN REDUCER: depthsetsize: "+depthSet.size()+" bafsetsize: "+bafSet.size()+" median depth: "+medianDepth+" median baf: "+medianBaf);
    // for median depth
    //retText.set(Integer.toString(startbp)+"\t"+Integer.toString(endbp)+"\t"+Double.toString(medianDepth)+"\t"+Double.toString(medianBaf)+"\t"+Double.toString(total_depth)+"\t"+Integer.toString(n));
    retText.set(Integer.toString(startbp)+"\t"+Integer.toString(endbp)+"\t"+Double.toString(medianDepth)+"\t"+bafVecStr+"\t"+Double.toString(total_depth)+"\t"+Integer.toString(n));
    ctx.write(inkey, retText);
  }
}

/**
 * This class is a wrapper class for invoking the HMM on a chromosome length
 * region
 */
class CnvReducer
extends Reducer<RefBinKey,Text,
               Text,Text> {
  private final Text outKey = new Text();
  private final Text textRes = new Text();
/**
 * @param inkey A composite key consisting of Chromosome and Bin ID
 * @param invals A list of string tuples where the elements in order are the  start base of the bin, the end base of the bin, the median bin depth, a vector of mean squared errors from expected BAFs for each of the four HMM states, the total depth of the bin, and the number of bases in the bin.
 */
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
  

//class AlleleDepthReducer
//extends Reducer<RefPosBaseKey,DoubleWritable,
//               RefPosBaseKey,DoubleWritable> {
//
//  private DoubleWritable doubleWritable = null;
//
//  public AlleleDepthReducer(){
//    doubleWritable = new DoubleWritable();
//  }
//  @Override protected void reduce(RefPosBaseKey inkey,Iterable<DoubleWritable> invals, Reducer<RefPosBaseKey,DoubleWritable, RefPosBaseKey,DoubleWritable>.Context ctx)
//    throws InterruptedException, IOException{
//      Iterator<DoubleWritable> it = invals.iterator();
//      double sum = 0.;
//      while(it.hasNext()){
//        sum+= it.next().get();
//      }      
//
//      if(sum>=1.0){
//        doubleWritable.set(sum);
//         ctx.write(inkey,doubleWritable);  
//      }
//      //System.err.println("REDUCER: "+inkey.toString()+": "+sum);
//    }
//}


/** 
 * A reducer class for efficiently computing the quality score weighted depth 
 * counts for each base in a read view. 
 */

class AlleleDepthWindowReducer
extends Reducer<RefBinKey,ArrayPrimitiveWritable,
               RefPosBaseKey,DoubleWritable> {
  private DoubleWritable doubleWritable = null;
  private int bin_size = Constants.read_bin_width;
  private List<Map<Integer,Double> > qualitymap_list = null;
  private RefPosBaseKey refPosBaseKey = null;
 
  public AlleleDepthWindowReducer(){
    refPosBaseKey = new RefPosBaseKey();
    doubleWritable = new DoubleWritable();
    qualitymap_list = new ArrayList<Map<Integer,Double> >(bin_size);
    // initialize the List
    for(int i=0;i<bin_size;++i){
      qualitymap_list.add(null);
      //qualitymap_list.add(new HashMap<Integer,Double>());
    }
  }

/**
 * @param inkey A composite key consisting of Chromosome and the first base pair position of a view.
 * @param invals A list of byte arrays, where each array corresponds to an aligned read that spans a view. @see Mappers
 * @param ctx A handle to the Reducer context for emitting the output.
 */
  @Override protected void reduce(RefBinKey inkey,Iterable<ArrayPrimitiveWritable> invals, Reducer<RefBinKey,ArrayPrimitiveWritable, RefPosBaseKey,DoubleWritable>.Context ctx)
    throws InterruptedException, IOException{
      for(int i=0;i<bin_size;++i){
        qualitymap_list.set(i,null);
      }
      boolean debug = false;
      int bin_size = Constants.read_bin_width;
      refPosBaseKey.setRefName(inkey.getRefName());
      //refPosBaseKey.setBase(1);
      //doubleWritable.set(0);
      //for(int i=0;i<bin_size;++i){
        //refPosBaseKey.setPosition(inkey.getBin() + i + 1);
        //ctx.write(refPosBaseKey,doubleWritable);  
      //}
if(true){
      Iterator<ArrayPrimitiveWritable> it = invals.iterator();
      //double sum = 0.;
      double quality_threshold=Constants.base_quality_threshold;
      int reduce_list_size = 0;
      while(it.hasNext()){
        //sum+= it.next().get();
        if(debug)System.err.print("KEY:"+inkey.toString());
        byte[] base_info = (byte[])it.next().get();
        for(int i=0;i<bin_size;++i){
          int allele = (int)base_info[i*2];
          int basequal =  (int)base_info[i*2+1];
          if(allele==0){
            if(debug)System.err.print(" 00");
          }else{
            if(debug)System.err.print(" "+allele);
            double qual = 1.-Math.pow(10,-basequal*.1);
            if(qual > quality_threshold){
              //qual = 1.0;
              Map<Integer,Double> map = qualitymap_list.get(i);
              Double val = null ;
              if(map==null) {
                map = new HashMap<Integer,Double>();
                val = qual;
              }else{
                val = map.get(allele);
                val= (val==null)?qual:val+qual;
              }
              map.put(allele,val);
              qualitymap_list.set(i,map);
            }
        
          }
        }
        if(debug)System.err.println();
        ++reduce_list_size;
      }      
      for(int i=0;i<bin_size;++i){
        Map<Integer,Double> map = qualitymap_list.get(i);
        if(map!=null){
          // convert things base to 1-based
          refPosBaseKey.setPosition(inkey.getBin() + i + 1);
          Set<Map.Entry<Integer,Double>> set = map.entrySet();
          Iterator<Map.Entry<Integer,Double>> it2 = set.iterator();
          while(it2.hasNext()){
            Map.Entry<Integer,Double> entry = it2.next();
            Integer allele = entry.getKey();
            Double depth = entry.getValue();
            //depth = (double)reduce_list_size;
            refPosBaseKey.setBase(allele);
            doubleWritable.set(depth);
            ctx.write(refPosBaseKey,doubleWritable);  
            if(debug) System.err.println("EMIT: "+refPosBaseKey.toString()+" "+allele+" "+depth);
          }
        }
      }
      //if(sum>=1.0) ctx.write(outkey,new DoubleWritable(sum));  
      //System.err.println("REDUCER: "+inkey.toString()+": "+sum);
    }
}
}


