package edu.usc;

//import sun.management.VMManagement;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;
import java.util.HashMap;
import java.util.NoSuchElementException;
import java.util.List;
import java.util.Vector;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

public class VcfLookup{

  public static final String SER_FILE="vcf.ser";
  public static final String TXT_FILE="workdir/depth/vcf.txt";
  
  private static volatile HashMap<String,HashSet<Integer>> hashMapSet = new HashMap<String,HashSet<Integer>>();
  private static volatile boolean hashMapSetInitialized = false;

  public VcfLookup(){
  }
 
 
/**
 * Check to see if a particular SNP exists in the tree structure
 * @param key The chromosome
 * @param value The base pair position
 * @return true or false
 */
  public boolean exists(String key, int value){
    HashSet<Integer> hashSet = hashMapSet.get(key);
    if(hashSet==null) return false;
    return hashSet.contains(value);    
  }

/**
 * Deserializes the tree structure from HDFS into memory
 */

  public synchronized void readObject(){
//    try {
//      RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
//      Field jvmField = runtimeMXBean.getClass().getDeclaredField("jvm");
//      jvmField.setAccessible(true);
//      VMManagement vmManagement = (VMManagement) jvmField.get(runtimeMXBean);
//      Method getProcessIdMethod = vmManagement.getClass().getDeclaredMethod("getProcessId");
//      getProcessIdMethod.setAccessible(true);
//      Integer processId = (Integer) getProcessIdMethod.invoke(vmManagement);
//      System.out.println("################    ProcessId = " + processId);
//    }catch (Exception e) {
//      e.printStackTrace();
//    }
    System.err.println("Entering readObject");
    if(hashMapSetInitialized){
      System.err.println("Exiting readObject early");
      return;
    }
    System.err.println("Loading from serialized file\n");
    //hashMapSet.clear();
    ObjectInputStream ois=null;
    hashMapSetInitialized = true;
    try{
      FileSystem fileSystem = FileSystem.get(new Configuration());
      Path path = new Path(SER_FILE);
      ois = new ObjectInputStream(fileSystem.open(path));
      this.hashMapSet = (HashMap<String, HashSet<Integer> >)ois.readObject();
    }catch(Exception ex){
      System.err.println("Printing the stack trace 1\n");
      ex.printStackTrace(System.err);
    }finally{
      try{
        ois.close(); 
      }catch(IOException ex){
        System.err.println("Printing the stack trace 2\n");
        ex.printStackTrace(System.err);
      }
    }
    System.err.println("Done reading serialized file");
    hashMapSetInitialized = true;
  }

/**
 * Insert a datapoint into the tree structure
 * @param key The chromosome
 * @param value the position
 */

  public void insertKeyValue(String key, int value){
    HashSet<Integer> hashSet = hashMapSet.get(key);
    if(hashSet==null) hashSet = new HashSet<Integer>();
    hashSet.add(value);
    hashMapSet.put(key,hashSet);
  }

/**
 * Parses a VCF file from the local file system and creates an in-memory
 * tree structure. It also serializes this tree to HDFS
 * @param infile The local file path of the VCF file
 */
  
  public void parseVcf(String infile)throws IOException{
    //hashMapSet.clear();
    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(infile)));
    String line;
    //String lastChr = "";
    while((line=reader.readLine())!=null){
      if (line.charAt(0)!='#'){
        String parts[] = line.split("\t");
        //System.out.println(parts[0]+" "+parts[1]);
        insertKeyValue(parts[0],Integer.parseInt(parts[1]));
      }
    }
    reader.close();
    FileSystem fileSystem = FileSystem.get(new Configuration());
    fileSystem.delete(new Path(SER_FILE),true);
    Path path = new Path(SER_FILE);
    ObjectOutputStream oos = new ObjectOutputStream(fileSystem.create(path));
    oos.writeObject(hashMapSet);
    oos.close();
    System.err.println("VCF file parsed, and serialized object persisted");
  }

/**
 * Parses a VCF from local file system and stores a tab-delimited file
 * in HDFS at /user/<homedir>/workdir/depth. The Depth call reducers also
 * write to this path. The Mapper for the Binner can then determine which sites
 * are SNPs as the intersection of the VCF file and the output of the Depth call
 * reducers.
 * @param infile The local file path of the VCF
 */

  public void parseVcf2Text(String infile)throws IOException{
    //hashMapSet.clear();
    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(infile)));
    String line;
    //String lastChr = "";
    FileSystem fileSystem = FileSystem.get(new Configuration());
    fileSystem.delete(new Path(TXT_FILE),true);
    Path path = new Path(TXT_FILE);
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fileSystem.create(path)));
    while((line=reader.readLine())!=null){
      if (line.charAt(0)!='#'){
        //int index0 = 0;
        //int index1 = line.indexOf((int)'\t',index0+1);
        //int index2 = line.indexOf((int)'\t',index1+1);
        //String ref = line.substring(index0,index1);
        //String pos = line.substring(index1+1,index2);
        String parts[] = line.split("\t");
        String ref = parts[0];
        String pos = parts[1];
        writer.write(ref+"\t"+pos+"\t0\t0\n");
        //writer.write(parts[0]+"\t"+parts[1]+"\t0\t0\n");
        //System.out.println(parts[0]+" "+parts[1]);
        //insertKeyValue(ref,Integer.parseInt(pos));
      }
    }
    reader.close();
    writer.close();
    System.err.println("VCF file parsed, and txt object persisted");
  }

  public void testRead() throws IOException,ClassNotFoundException{
    ObjectInputStream ois = new ObjectInputStream(new FileInputStream("vcf.ser"));
    HashMap<String, HashSet<Integer> > hashMapSet = (HashMap<String, HashSet<Integer> >)ois.readObject();
    ois.close();
    Iterator<String> it = hashMapSet.keySet().iterator();
    while(it.hasNext()){
      String key = it.next();
      HashSet<Integer> hashSet = hashMapSet.get(key);
      Iterator<Integer> it2 = hashSet.iterator();
      while(it2.hasNext()){
        Integer i = it2.next();
        System.out.println(key+" "+i);
      }
    }
    while(true);
  }

  public void testWrite() throws IOException,ClassNotFoundException{
    HashMap<String, HashSet<Integer> > hashMapSet = new HashMap<String,HashSet<Integer>>();
    System.err.println("writing hashmapset\n");
    String chrs[]={"chr1","chr2"};
    for(int i=0;i<chrs.length;++i){
      HashSet<Integer> posSet = new HashSet<Integer>();
      for(int j=0;j<400;++j){
        posSet.add(j);
      }
      hashMapSet.put(chrs[i],posSet);
    }
    System.err.println("Done!");
    ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream("vcf.ser"));
    oos.writeObject(hashMapSet);
    oos.close();
  }

  public static void main(String args[]){
    if(args.length<2){
      System.err.println("Usage: "+args[0]+" <VCF_file>");
      System.exit(1);
    }
    String infile = args[1];
    VcfLookup vcfLookup = new VcfLookup();
    try{
      vcfLookup.parseVcf(infile);
      boolean write = true;
      if(write){
        //vcfLookup.testWrite();
      }else{
        vcfLookup.testRead();
      }
    }catch(Exception ex){
      ex.printStackTrace();
    }
    
    return;
  }
}
