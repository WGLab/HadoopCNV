package edu.usc;


// the following constants must be set here, and source compiled, as the Mappers and Reducers that are run on the cluster are stateless (i.e. do not accept initializers)


public class Constants{
  // Read bin
  public static final int read_bin_width = 100;
  public static final double mapping_quality_threshold = .99;
  public static final double base_quality_threshold = .99;

  // Binner settings
  public static final int bin_width = 1;

  // HMM settings 
  // Penalties are non-negative. For negative penalties, 
  // a default will be estimated based on SD of the LRR signal
  public static final float abberration_penalty = -1f;
  public static final float transition_penalty = -1f;
  // Alpha is BAF mixture proportion in emission model. 
  // 0 is pure LRR signal, 1 is pure BAF signal
  public static final float alpha = 0.5f;

}
