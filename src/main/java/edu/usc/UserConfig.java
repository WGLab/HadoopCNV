package edu.usc;

import java.io.IOException;
import java.io.FileReader;
import java.util.Properties;

/**
 * Encapsulates the various settings stored in the run time configuration file
 * for PennCnvSeq. Convenience methods are provided to return the appropriate
 * type from a String.
 *
 * @author Gary Chen, Ph.D.
 * @revised by Max He, Ph.D.
 */
public class UserConfig {

    private static Properties prop;

    /**
     * Initialize this object with the local file path of the configuration
     *
     * @param filename the path
     * @throws java.io.IOException
     */
    public static void init(String filename)
            throws IOException {
        prop = new Properties();
        prop.load(new FileReader(filename));
    }

    /**
     * @return The fully qualified BAM path on HDFS. Can be a wildcard for
     * multiple BAMs.
     */
    public static String getBamFile() {
        return getConfig("BAM_FILE");
    }

    /**
     * @return The path for the VCF file on the local file system
     */
    public static String getVcfFile() {
        return getConfig("VCF_FILE");
    }

    /**
     * @return Should we run the VCF parser component?
     */
    public static boolean getInitVcf() {
        return Boolean.parseBoolean(getConfig("INIT_VCF_LOOKUP"));
    }

    /**
     * @return Should we run the depth caller component?
     */
    public static boolean getRunDepthCaller() {
        return Boolean.parseBoolean(getConfig("RUN_DEPTH_CALLER"));
    }

    /**
     * @return Should we run the binner component?
     */
    public static boolean getRunBinner() {
        return Boolean.parseBoolean(getConfig("RUN_BINNER"));
    }

    /**
     * @return Should we run the CNV caller component?
     */
    public static boolean getRunCnvCaller() {
        return Boolean.parseBoolean(getConfig("RUN_CNV_CALLER"));
    }
    
    /**
     * @return Whether to run the split-read  read extracter
     */
    public static boolean getRun_SR_PE_Extracter(){
    	return Boolean.parseBoolean(getConfig("RUN_SR_PE_EXTRACTER"));
    }

    
    /**
     * @return Should we run the global sort component? This sorts the depth
     * calls over all bases.
     */
    public static boolean getRunGlobalSort() {
        return Boolean.parseBoolean(getConfig("RUN_GLOBAL_SORT"));
    }

    /**
     * @return The number of Reducers to be used for the Depth caller
     */
    public static int getBamFileReducers() {
        return Integer.parseInt(getConfig("BAM_FILE_REDUCERS"));
    }

    /**
     * @return The number of Reducers to be used for all components except the
     * depth caller
     */
    public static int getTextFileReducers() {
        return Integer.parseInt(getConfig("TEXT_FILE_REDUCERS"));
    }

    public static String getYarnContainerMinMb() {
        return getConfig("YARN_CONTAINER_MIN_MB").trim();
    }

    public static String getYarnContainerMaxMb() {
        return getConfig("YARN_CONTAINER_MAX_MB").trim();
    }

    public static String getHeapsizeMinMb() {
        return getConfig("HEAPSIZE_MIN_MB").trim();
    }

    public static String getHeapsizeMaxMb() {
        return getConfig("HEAPSIZE_MAX_MB").trim();
    }

    public static String getMapperMb() {
        return getConfig("MAPPER_MB").trim();
    }

    public static String getReducerMb() {
        return getConfig("REDUCER_MB").trim();
    }

    public static int getReducerTasks() {
        return Integer.parseInt(getConfig("REDUCER_TASKS").trim());
    }
    public static float getAbberationPenalty(){
    	return Float.parseFloat(getConfig("ABBERATION_PENALTY").trim());
    }
    public static float getTransitionPenalty(){
    	return Float.parseFloat(getConfig("TRANSITION_PENALTY").trim());
    }
    
    private static String getConfig(String key) {
        return prop.getProperty(key);
    }
}
