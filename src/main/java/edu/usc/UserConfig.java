package edu.usc;

import java.io.Reader;
import java.io.IOException;
import java.io.FileReader;
import java.util.Properties;

public class UserConfig{
  private Properties prop;

  public UserConfig(){
    prop = new Properties();
  }

  public void init(String filename)
  throws IOException{
    prop.load(new FileReader(filename));
  }


  public String getBamFile(){
    return getConfig("BAM_FILE");
  }

  public String getVcfFile(){
    return getConfig("VCF_FILE");
  }

  public int getBinWidth(){
    return Integer.parseInt(getConfig("BIN_WIDTH"));
  }

  private String getConfig(String key){
    return prop.getProperty(key);
  }
  
}
