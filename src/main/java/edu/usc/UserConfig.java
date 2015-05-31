package edu.usc;

import java.io.Reader;
import java.io.IOException;
import java.io.FileReader;
import java.util.Properties;

public class UserConfig{
  private static Properties prop;

  //public UserConfig(){
  //  prop = new Properties();
  //}

  public static void init(String filename)
  throws IOException{
    prop = new Properties();
    prop.load(new FileReader(filename));
  }


  public static String getBamFile(){
    return getConfig("BAM_FILE");
  }

  public static String getVcfFile(){
    return getConfig("VCF_FILE");
  }
 
  public static boolean getInitVcf(){
    return Boolean.parseBoolean(getConfig("INIT_VCF_LOOKUP"));
  }

  public static boolean getRunDepthCaller(){
    return Boolean.parseBoolean(getConfig("RUN_DEPTH_CALLER"));
  }

  public static boolean getRunBinner(){
    return Boolean.parseBoolean(getConfig("RUN_BINNER"));
  }

  public static boolean getRunCnvCaller(){
    return Boolean.parseBoolean(getConfig("RUN_CNV_CALLER"));
  }

  public static boolean getRunGlobalSort(){
    return Boolean.parseBoolean(getConfig("RUN_GLOBAL_SORT"));
  }

  public static int getBamFileReducers(){
    return Integer.parseInt(getConfig("BAM_FILE_REDUCERS"));
  }

  public static int getTextFileReducers(){
    return Integer.parseInt(getConfig("TEXT_FILE_REDUCERS"));
  }

  public static String getYarnContainerMinMb(){
    return getConfig("YARN_CONTAINER_MIN_MB").trim();
  }

  public static String getYarnContainerMaxMb(){
    return getConfig("YARN_CONTAINER_MAX_MB").trim();
  }

  public static String getHeapsizeMinMb(){
    return getConfig("HEAPSIZE_MIN_MB").trim();
  }

  public static String getHeapsizeMaxMb(){
    return getConfig("HEAPSIZE_MAX_MB").trim();
  }

  public static String getMapperMb(){
    return getConfig("MAPPER_MB").trim();
  }

  public static String getReducerMb(){
    return getConfig("REDUCER_MB").trim();
  }

  private static String getConfig(String key){
    return prop.getProperty(key);
  }
  
}
