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

public class Reducers{
}

class BinReducer
extends Reducer<RefBinKey,Text,
               RefBinKey,Text> {
  
  private final Text retText = new Text();
  private final Map<Integer,Boolean > binMapInVcf = new HashMap();
  private final Map<Integer,List<Float> > binMapDepth = new HashMap();
  private final Set<Float> depthSet = new TreeSet();
  private final Set<Float> bafSet = new TreeSet();

  @Override 
  protected void reduce(RefBinKey inkey,Iterable<Text> invals,
  Reducer<RefBinKey,Text, RefBinKey,Text>.Context ctx)
  throws InterruptedException, IOException{
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
    double total_baf = 0;
    int n=0;
    int hets = 0;
    int baf_n = 0;
    while(it_keys.hasNext()){
      Integer bp = it_keys.next();
      boolean in_vcf = binMapInVcf.get(bp);
      Iterator<Float> depthListIterator = binMapDepth.get(bp).iterator();
      float maxDepth = 0,pos_depth=0;
      while(depthListIterator.hasNext()){
        float depth = depthListIterator.next().floatValue();
        pos_depth+=depth;
        if(depth>maxDepth) maxDepth = depth;
      }
      float baf = (in_vcf)?(pos_depth-maxDepth)/pos_depth:0f;
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
    //retText.set(Integer.toString(startbp)+"\t"+Integer.toString(endbp)+"\t"+Double.toString(mean_depth) +"\t"+Double.toString(mean_baf) +"\t"+Double.toString(total_depth)+"\t"+Integer.toString(n));
    retText.set(Integer.toString(startbp)+"\t"+Integer.toString(endbp)+"\t"+Double.toString(medianDepth)+"\t"+Double.toString(medianBaf)+"\t"+Double.toString(total_depth)+"\t"+Integer.toString(n));
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
  DoubleWritable doubleWritable = null;
  public AlleleDepthReducer(){
    doubleWritable = new DoubleWritable();
  }
  @Override protected void reduce(RefPosBaseKey inkey,Iterable<DoubleWritable> invals, Reducer<RefPosBaseKey,DoubleWritable, RefPosBaseKey,DoubleWritable>.Context ctx)
    throws InterruptedException, IOException{
      Iterator<DoubleWritable> it = invals.iterator();
      double sum = 0.;
      while(it.hasNext()){
        sum+= it.next().get();
      }      

      if(sum>=1.0){
        doubleWritable.set(sum);
         ctx.write(inkey,doubleWritable);  
      }
      //System.err.println("REDUCER: "+inkey.toString()+": "+sum);
    }
}


// this version uses an array that roughly is the size of reads with
// the hope that there are less maps and reduces to do

class AlleleDepthWindowReducer
extends Reducer<RefBinKey,ArrayPrimitiveWritable,
               RefPosBaseKey,DoubleWritable> {
  RefPosBaseKey refPosBaseKey = null;
  DoubleWritable doubleWritable = null;
  List<Map<Integer,Double> > qualitymap_list = null;
  int bin_size = Constants.read_bin_width;
 
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


