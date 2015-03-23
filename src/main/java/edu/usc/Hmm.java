package edu.usc;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.List;
import java.util.Vector;
import java.util.Arrays;

import org.apache.hadoop.io.Text;

public class Hmm{
  private String refName;
  private final int states = 4;
  private int markers;
  private long [] ranges;
  private float [] data;
  private float [] lrr_mat;
  private float [] baf_mat;
  private float [] loss_mat;
  private float [] scaled_depth;
  private float mean_coverage;
  private float[] mu = {-.5f,0,0,.5f};
  private int[] cn_arr = {1,2,2,3};
  private int [] best_state;
  private float alpha=.5f;
  private float  lambda1=0.1f;
  private float lambda2=0.1f;

  public Hmm(int markers){
    this.markers = markers;
  }
  public Hmm(){
  }

  public void setAlpha(float alpha){
    this.alpha = alpha;
  }

  public void setLambda1(float lambda1){
    this.lambda1 = lambda1;
  }

  public void setLambda2(float lambda2){
    this.lambda2 = lambda2;
  }

  private void init_matrices(){
    lrr_mat = new float[markers*states];
    baf_mat = new float[markers*states];
    loss_mat = new float[markers*states];
    best_state = new int[markers];
    scaled_depth = new float[markers];
    scale_depth();
    float sd = get_sd();
    System.err.println("Standard deviation is "+sd);
    this.lambda1 = sd;
    this.lambda2 = 2*sd*(float)Math.sqrt(Math.log(markers));
    lambda1*=1;
    lambda2*=1;
    System.err.println("Chr "+refName+" lambda 1 and 2 are "+lambda1+" and "+lambda2);
  }

  private float get_sd(){
    if(markers<2) return 0;
    float mean = 0;
    for(int marker=0;marker<markers;++marker){
      mean+=scaled_depth[marker];
    }
    mean/=markers;
    float sd = 0;
    for(int marker=0;marker<markers;++marker){
      float dev = (scaled_depth[marker]-mean);
      sd+=dev*dev;
    }
    sd = (float)Math.sqrt(sd/(markers-1));
    return sd;
  }

  private void scale_depth(){
    //mean_coverage = 0;
    //for(int i=0;i<markers;++i){
      //mean_coverage+=data[i*2];
    //}
    //mean_coverage/=markers;
    //System.err.println("Mean coverage: "+mean_coverage);
    for(int i=0;i<markers;++i){
      scaled_depth[i] = (data[i*2]-mean_coverage)/mean_coverage;
      if(scaled_depth[i]< -.5) scaled_depth[i] = -.5f;
      if(scaled_depth[i]>  .5) scaled_depth[i] =  .5f;
    }
    
  }
  
  private void compute_emission(){
    if(markers<2) return;
    for(int i=0;i<markers;++i){
      for(int state=0;state<states;++state){
        float dev = scaled_depth[i] - mu[state];
        lrr_mat[i*states+state] = dev*dev;
        if(state==0 || state==1){
          baf_mat[i*states+state] = Math.abs(data[i*2+1] - 0); // fill this in later
        }else{
          baf_mat[i*states+state] = Math.abs(data[i*2+1] - 0.1f); // fill this in later
        }
      }
    }
  }

  private void do_viterbi(){
    int[] traceback = new int[markers*states];
    float min = Float.MAX_VALUE;
    for(int state=0;state<states;++state){
      loss_mat[state] = lrr_mat[state]+alpha*baf_mat[state]+lambda1*Math.abs(mu[state]);
      if(loss_mat[state]<min) min = loss_mat[state];
    }
    for(int marker=1;marker<markers;++marker){
      float dist = 1;
      for(int current=0;current<states;++current){
        min = Float.MAX_VALUE;
        float loss = 0;
        for(int prev=0;prev<states;++prev){
          loss = loss_mat[(marker-1)*states+prev]+lrr_mat[marker*states+
          current]+alpha * baf_mat[marker*states+current]+lambda1 *
          Math.abs(mu[current]) + lambda2*(dist)*Math.abs(mu[current]- mu[prev]);
          if(loss<min) {
            min = loss;
            loss_mat[marker*states+current] = min;
            traceback[marker*states+current] = prev;
          }
        }
        if(min==1e10) {
          System.err.println("A Failed to find minimum:");
          for(int prev=0;prev<states;++prev){
            loss = loss_mat[(marker-1)*states+prev]+lrr_mat[marker*states+
            current]+alpha * baf_mat[marker*states+current]+lambda1 *
            Math.abs(mu[current]) + lambda2*(dist)*Math.abs(mu[current]- mu[prev]);
            System.err.println(" "+prev+","+loss);
          }
          System.err.println();
          System.exit(1);
        }
      }
    }
    // now do the traceback
    int min_index=1; 
    min=Float.MAX_VALUE;
    for(int current=0;current<states;++current){
      if(loss_mat[(markers-1)*states+current]<min){
        min = loss_mat[(markers-1)*states+current];
        min_index = current;
      }
    }
    if(min==Float.MAX_VALUE) {
      System.err.println("B Failed to find minimum!");
      System.exit(1);
    }
    best_state[markers-1] = cn_arr[min_index];
    for(int marker = markers-1;marker>0;--marker){
      best_state[marker-1] = traceback[marker*states+min_index];
    }
  }
  
  // returns -1 if small CNVs found, 1 if big CNVs found, 0 else
  
  private int good_cnv_size(){
    // these are user preferences for CNV distributions
    //int abb_count_threshold_min = 100;
    int length_threshold_min  = (int)(markers*.01);
    int length_threshold_max =  (int)(markers*.1);
    //int abb_count_threshold_max = 1000;
    System.err.println("Good CNV range: "+length_threshold_min+" to "+length_threshold_max);

    int last_cn = 2;
    int abb_start = -1;
    int abb_end = -1;
    int size_flag=0;
    //int under_min_abberation = 0;
    //int over_max_abberation = 0;
    int total_abb_len = 0;
    for(int marker = 0;marker< markers;++marker){
      int cur_cn = cn_arr[best_state[marker]];
      if(last_cn ==2 && cur_cn!=2) {
        abb_start = marker;
        //System.err.println("Abberation start at marker "+marker);
      }else if(last_cn!=2 && cur_cn==2) {
        abb_end = marker-1;
        //System.err.println("Abberation end at marker "+abb_end);
        int len = abb_end-abb_start+1;
        total_abb_len+=len;
        //if (len>length_threshold_max) ++over_max_abberation;
        //if (len<length_threshold_min) ++under_min_abberation;
         
      }
      last_cn = cur_cn;
    }
    // nothing was found, too strong a penalty
    //if (abb_start==-1) return 1;
    System.err.println("Abberation len "+total_abb_len);
    if(total_abb_len>length_threshold_max){
      System.err.println("Abberation len too big, aborting"); 
      size_flag = -1;
    }else if(total_abb_len<length_threshold_min){
      System.err.println("Abberation len too small, aborting"); 
      size_flag = 1;
    }
    return size_flag;
  }

  public Iterator<String> getResults(){
    return new ResultIterator();
  }

  public void print_output(){
    System.out.println("start\tend\tscaled_depth\tCN\tstate");
    Iterator<String> it_result = getResults();
    while(it_result.hasNext()){
      System.out.println(it_result.next());
    //for(int marker=0;marker<markers;++marker){
      //int cn = cn_arr[best_state[marker]];
      //System.out.println(ranges[marker*2]+"\t"+ranges[marker*2+marker]+"\t"+
      //scaled_depth[marker]+"\t"+cn+"\t"+ best_state[marker]);
    }
  }

  private void search(float lambda2_min, float lambda2_max){
    float mid = (lambda2_max-lambda2_min)/2;
    this.lambda2 = lambda2_min+mid;
    System.err.println("Searching range "+lambda2_min+" to "+lambda2_max+" lambda2 is "+lambda2);
    if(mid < .01) {
      System.err.println("Too small a range, aborting");
      return;
    }
    do_viterbi();
    int size_flag = good_cnv_size();
    if (size_flag==0) return;
    else if(size_flag<0) search(lambda2,lambda2_max);
    else if(size_flag>0) search(lambda2_min,lambda2);
  }

  public void run(){
    compute_emission();
    search(0,lambda2*2);
  }

  /*
 * entrypoint from Hadoop reduce task
 */
  public void init(String refName, Iterator<Text> it_text){
    this.refName = refName;
    Vector<Long> start = new Vector<Long>();
    Vector<Long> end = new Vector<Long>();
    Vector<Float> depth = new Vector<Float>();
    Vector<Float> baf = new Vector<Float>();
    int sites = 0;
    mean_coverage = 0;
    
    
    while(it_text.hasNext()){
      String reduceValue = it_text.next().toString();
      //System.err.println("CNV REDUCER VALUE: "+reduceValue);
      String [] parts = reduceValue.split("\t");
      start.add(Long.decode(parts[0]));
      end.add(Long.decode(parts[1]));
      depth.add(Float.valueOf(parts[2]));
      int bac = Integer.valueOf(parts[3]); 
      mean_coverage+=(Float.valueOf(parts[4]));
      sites+=(Integer.valueOf(parts[5]));
      baf.add((float)bac/Integer.valueOf(parts[5]));
    }
    //Object[] depth_arr = depth.toArray();
    //Arrays.sort(depth_arr);
    //mean_coverage = (double)depth_arr[depth_arr.length/2];
    mean_coverage/=sites;
    //mean_coverage = 40;
    this.markers = start.size();
    //System.err.println("TOTAL MARKERS: "+markers);
    System.err.println("Chr "+refName+" mean coverage of "+markers+" markers is "+mean_coverage);
    ranges = new long[markers*2];
    data = new float[markers*2];
    for(int i=0;i<markers;++i){
      ranges[i*2] = start.get(i);
      ranges[i*2+1] = end.get(i);
      data[i*2] = depth.get(i);
      data[i*2+1] = baf.get(i);
    }
    init_matrices();
  }

  public void init(String refName,long[] ranges,float [] data, float mean_coverage){
    this.mean_coverage = mean_coverage;
    this.refName = refName;
    this.ranges = ranges;
    this.data = data;
    init_matrices();
  }

  public static void main(String args[]){
    try{
      String filename = args[1];
      List<String> lines = Files.readAllLines(Paths.get(filename),Charset.defaultCharset());
      int markers = lines.size();
      System.err.println("There are "+lines.size()+" lines in "+filename);
      Hmm hmm = new Hmm(markers);
      long[] ranges = new long[markers*2];
      float[] data = new float[markers*2];
      Iterator<String> lines_it = lines.iterator();
      int marker=0;

      String refName="";
      float mean_coverage = 0;
      int sites= 0;
      while(lines_it.hasNext()){
        String []parts = lines_it.next().split("\t");
        int partindex = 0;
        refName = parts[partindex++];
        ++partindex;
        ranges[marker*2] = Long.parseLong(parts[partindex++]);
        ranges[marker*2+1] = Long.parseLong(parts[partindex++]);
        data[marker*2] = Float.parseFloat(parts[partindex++]);
        int bac = Integer.parseInt(parts[partindex++]);
        float totalDepth = Float.parseFloat(parts[partindex++]);
        mean_coverage+=totalDepth;
        int binMarkers = Integer.parseInt(parts[partindex++]);
        sites+=binMarkers;
        data[marker*2+1] = (float)bac/binMarkers;
        //System.err.println("marker "+ranges[marker*2]+" with depth "+data[marker*2]+" and baf "+data[marker*2+1]);
        ++marker;
      }
      mean_coverage/=sites;
      System.err.println("Chr "+refName+" mean coverage of "+markers+" markers is "+mean_coverage);
      hmm.init(refName,ranges,data,mean_coverage);
      hmm.run();
      hmm.print_output();
    }catch(Exception ex){
      ex.printStackTrace();
      System.exit(1);
    }

  }

  class ResultIterator implements Iterator<String>{
    private int marker = 0;

    public ResultIterator(){
    }

    @Override
    public boolean hasNext(){
      return marker<=markers-1;
    }

    @Override
    public String next(){ 
      if (marker>=markers) throw new NoSuchElementException();
      String res = Long.toString(ranges[marker*2])+"\t"+Long.toString(ranges[marker*2+1])+"\t"+Float.toString(scaled_depth[marker])+"\t"+Integer.toString(cn_arr[best_state[marker]])+"\t" + Integer.toString(best_state[marker]);
      ++marker;
      return res;
    }

    @Override
    public void remove(){
      throw new UnsupportedOperationException();
    }
    
  }
}
