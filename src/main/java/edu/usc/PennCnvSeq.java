package edu.usc;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.seqdoop.hadoop_bam.AnySAMInputFormat;

/**
 * PennCnvSeq is the entry point for the Hadoop based implementation of the
 * PennCnvSeq program. PennCnvSeq expects a single argument, which is the name
 * of the configuration file that specifies where the VCF and BAM files are, and
 * which components of the pipeline to run.
 *
 * @author Gary Chen, Ph.D.
 * @revised by Max He, Ph.D.
 */
public class PennCnvSeq extends Configured implements Tool {

//    public static final String JAVA_VENDOR_NAME = System.getProperty("java.vendor");
//    public static final boolean IBM_JAVA = JAVA_VENDOR_NAME.contains("IBM");
    Configuration conf;
    Path bamfile, vcffile;
    String bamfileStr, vcffileStr;
    static long startTime, endTime;
    boolean debug = false;  // true;

    /**
     * The default constructor for PennCvnSeq. No other initializations occur
     * here.
     */
    public PennCnvSeq() {
    }

    /**
     * This method implements the same method specified in the Tool interface.
     * It takes in the necessary user specified arguments, ignoring Hadoop
     * arguments such as the jar file name.
     *
     * @param args	the String array that was passed in to the main function
     * @return	The exit code
     */
    @Override
    public int run(String args[]) {
//        for (int i = 0; i < args.length; ++i) {
//            System.out.println("Argument " + i + ": " + args[i]);
//        }
//
        this.conf = super.getConf();

        FileSystem fileSystem;  // = null;

        try {
            UserConfig.init(args[0]);
        } catch (IOException ex) {
            System.err.println("Cannot open configuration file " + args[0]);
            ex.printStackTrace(System.err);
            System.exit(1);
        }

        if (conf == null) {
            System.err.println("Configuration null!");
        }

        try {
            this.bamfileStr = UserConfig.getBamFile();
            File bamFile = new File(bamfileStr);
            String subFolderName = bamFile.getName().substring(0, bamFile.getName().lastIndexOf("."));
            String depthFolder = "workdir" + java.io.File.separator + "depth" + java.io.File.separator + subFolderName;
            String sortedFolder = "workdir" + java.io.File.separator + "sorted" + java.io.File.separator + subFolderName;
            String partitioningFolder = "workdir" + java.io.File.separator + "partitioning" + java.io.File.separator + subFolderName;
            String binsFolder = "workdir" + java.io.File.separator + "bins" + java.io.File.separator + subFolderName;
            String cnvFolder = "workdir" + java.io.File.separator + "cnv" + java.io.File.separator + subFolderName;
//            if (debug) {
//                System.out.println("\nbamfileStr: " + bamfileStr + " subFolderName: " + subFolderName
//                        + " depthFolder: " + depthFolder + " sortedFolder: " + sortedFolder
//                        + " partitioningFolder: " + partitioningFolder
//                        + " binsFolder: " + binsFolder + " cnvFolder: " + cnvFolder);
//            }
//            System.exit(0);
//
            int numReduceTasksBig = UserConfig.getBamFileReducers();
            int numReduceTasksSmall = UserConfig.getTextFileReducers();
            boolean runVcfLookup = UserConfig.getInitVcf();
            boolean runDepthCallJob = UserConfig.getRunDepthCaller();
            boolean runRegionBinJob = UserConfig.getRunBinner();
            boolean runCnvCallJob = UserConfig.getRunCnvCaller();
            boolean runGlobalSortJob = UserConfig.getRunGlobalSort();
            int numRecudeTasks = UserConfig.getReducerTasks();
            if (debug) {
                System.err.println("numReduceTasksBig: " + numReduceTasksBig
                        + " numReduceTasksSmall: " + numReduceTasksSmall
                        + " runVcfLookup: " + runVcfLookup
                        + " runDepthCallJob: " + runDepthCallJob
                        + " runRegionBinJob: " + runRegionBinJob
                        + " runCnvCallJob: " + runCnvCallJob
                        + " runGlobalSortJob: " + runGlobalSortJob);
            }

            fileSystem = FileSystem.get(conf);
            if (runDepthCallJob) {
                //conf.setInt("dfs.blocksize",512*1024*1024);
                //conf.set("yarn.scheduler.minimum-allocation-mb",UserConfig.getYarnContainerMinMb());
                //System.out.println("Memory configuration YARN min MB: "+conf.get("yarn.scheduler.minimum-allocation-mb"));

                //conf.set("yarn.scheduler.maximum-allocation-mb",UserConfig.getYarnContainerMaxMb());
                //System.out.println("Memory configuration YARN max MB: "+conf.get("yarn.scheduler.maximum-allocation-mb"));
                //conf.set("mapred.child.java.opts", "-Xms"+UserConfig.getHeapsizeMinMb()+"m -Xmx"+UserConfig.getHeapsizeMaxMb()+"m");
                //System.out.println("Memory configuration Heap in MB: "+conf.get("mapred.child.java.opts"));
                //conf.set("mapreduce.map.memory.mb",UserConfig.getMapperMb());
                //System.out.println("Memory configuration Mapper MB: "+conf.get("mapreduce.map.memory.mb"));
                //conf.set("mapreduce.reduce.memory.mb",UserConfig.getReducerMb());
                //System.out.println("Memory configuration Reducer MB: "+conf.get("mapreduce.reduce.memory.mb"));
//                this.bamfileStr = UserConfig.getBamFile();
                this.bamfile = new Path(bamfileStr);
                System.out.println("Bamfile is at " + bamfile);
//                Job depthCallJob = new Job(conf);
                Job depthCallJob = Job.getInstance(conf, "Depth Call Job");
                depthCallJob.setJobName("depth_caller");
                depthCallJob.setNumReduceTasks(numReduceTasksBig);
                depthCallJob.setInputFormatClass(AnySAMInputFormat.class);
                depthCallJob.setJarByClass(PennCnvSeq.class);
                boolean use_window_mapper = true;
                if (use_window_mapper) {
                    depthCallJob.setMapperClass(SAMRecordWindowMapper.class);
                    depthCallJob.setMapOutputKeyClass(RefBinKey.class);
                    depthCallJob.setMapOutputValueClass(ArrayPrimitiveWritable.class);
                    depthCallJob.setReducerClass(AlleleDepthWindowReducer.class);
                } else {
                    System.out.println("No window mapper version not implemented.");
                    return -1;
                }
                depthCallJob.setOutputKeyClass(RefPosBaseKey.class);
                depthCallJob.setOutputValueClass(DoubleWritable.class);
                FileInputFormat.addInputPath(depthCallJob, bamfile);
//                fileSystem.delete(new Path("workdir/depth"), true);
//                FileOutputFormat.setOutputPath(depthCallJob, new Path("workdir/depth"));
                fileSystem.delete(new Path(depthFolder), true);
                FileOutputFormat.setOutputPath(depthCallJob, new Path(depthFolder));
                System.out.println("Submitting read depth caller.");
                if (!depthCallJob.waitForCompletion(true)) {
                    System.out.println("Depth caller failed.");
                    return -1;
                }

                if (debug) {
                    System.out.println("Done of submitting read depth caller.");
                    endTime = System.currentTimeMillis();
                    String runningTime = "Running Time: " + Math.rint((endTime - startTime) * 100) / 100000.0 + " seconds.\n";
                    System.out.println(runningTime + "\n");
                }
            }

            if (runGlobalSortJob) {
                int numReduceTasks = numReduceTasksSmall;
                double pcnt = 10.0;
                int numSamples = numReduceTasks;
                int maxSplits = numReduceTasks - 1;
                if (0 >= maxSplits) {
                    maxSplits = Integer.MAX_VALUE;
                }
//                InputSampler.Sampler sampler = new InputSampler.RandomSampler(pcnt, numSamples, maxSplits);
                InputSampler.Sampler<String, String> sampler = new InputSampler.RandomSampler<>(pcnt, numSamples, maxSplits);

//                Job regionSortJob = new Job(conf);
                Job regionSortJob = Job.getInstance(conf, "Region Sort Job");
                regionSortJob.setJobName("sorter");
                regionSortJob.setJarByClass(PennCnvSeq.class);
                regionSortJob.setNumReduceTasks(numReduceTasks);
                regionSortJob.setInputFormatClass(SortInputFormat.class);
                regionSortJob.setMapperClass(Mapper.class);
                regionSortJob.setMapOutputKeyClass(RefPosBaseKey.class);
                regionSortJob.setMapOutputValueClass(Text.class);
                regionSortJob.setReducerClass(Reducer.class);
                regionSortJob.setOutputKeyClass(RefPosBaseKey.class);
                regionSortJob.setOutputValueClass(Text.class);
//                FileInputFormat.addInputPath(regionSortJob, new Path("workdir/depth/"));
//                fileSystem.delete(new Path("workdir/sorted"), true);
//                FileOutputFormat.setOutputPath(regionSortJob, new Path("workdir/sorted/"));
                FileInputFormat.addInputPath(regionSortJob, new Path(depthFolder));
                fileSystem.delete(new Path(sortedFolder), true);
                FileOutputFormat.setOutputPath(regionSortJob, new Path(sortedFolder));
                regionSortJob.setPartitionerClass(TotalOrderPartitioner.class);
//                TotalOrderPartitioner.setPartitionFile(regionSortJob.getConfiguration(), new Path("workdir/partitioning"));
                TotalOrderPartitioner.setPartitionFile(regionSortJob.getConfiguration(), new Path(partitioningFolder));
                InputSampler.writePartitionFile(regionSortJob, sampler);
                System.out.println("Submitting global sort.");
                if (!regionSortJob.waitForCompletion(true)) {
                    System.out.println("Global sort failed.");
                    return 1;
                }

                if (debug) {
                    System.out.println("Done of submitting global sort.");
                    endTime = System.currentTimeMillis();
                    String runningTime = "Running Time: " + Math.rint((endTime - startTime) * 100) / 100000.0 + " seconds.\n";
                    System.out.println(runningTime + "\n");
                }
            }

            if (runVcfLookup) {
                this.vcffileStr = UserConfig.getVcfFile();
                this.vcffile = new Path(vcffileStr);
                System.out.println("Vcffile is at " + vcffile);
                VcfLookup lookup = new VcfLookup();
                lookup.parseVcf2Text(vcffileStr);

                if (debug) {
                    System.out.println("Done of runVcfLookup.");
                    endTime = System.currentTimeMillis();
                    String runningTime = "Running Time: " + Math.rint((endTime - startTime) * 100) / 100000.0 + " seconds.\n";
                    System.out.println(runningTime + "\n");
                }
            }

            if (runRegionBinJob) {
                Job regionBinJob = Job.getInstance(conf, "Region Bin Job");
                regionBinJob.setJobName("binner");
                regionBinJob.setJarByClass(PennCnvSeq.class);
                regionBinJob.setNumReduceTasks(numReduceTasksSmall);
                regionBinJob.setInputFormatClass(TextInputFormat.class);
                regionBinJob.setMapperClass(BinMapper.class);
                regionBinJob.setMapOutputKeyClass(RefBinKey.class);
                regionBinJob.setMapOutputValueClass(Text.class);
                regionBinJob.setReducerClass(BinReducer.class);
                regionBinJob.setOutputKeyClass(RefBinKey.class);
                regionBinJob.setOutputValueClass(Text.class);

//                FileInputFormat.addInputPath(regionBinJob, new Path("workdir/depth/"));
//                fileSystem.delete(new Path("workdir/bins"), true);
//                FileOutputFormat.setOutputPath(regionBinJob, new Path("workdir/bins"));
                FileInputFormat.addInputPath(regionBinJob, new Path(depthFolder));
                fileSystem.delete(new Path(binsFolder), true);
                FileOutputFormat.setOutputPath(regionBinJob, new Path(binsFolder));
                System.out.println("Submitting binner.");
                if (!regionBinJob.waitForCompletion(true)) {
                    System.out.println("Binner failed.");
                    return -1;
                }

                if (debug) {
                    System.out.println("Done of submitting binner.");
                    endTime = System.currentTimeMillis();
                    String runningTime = "Running Time: " + Math.rint((endTime - startTime) * 100) / 100000.0 + " seconds.\n";
                    System.out.println(runningTime + "\n");
                }
            }

            if (runCnvCallJob) {
                Job secondarySortJob = Job.getInstance(conf, "Secondary Sort Job");
                secondarySortJob.setJobName("secondary_sorter");
                secondarySortJob.setJarByClass(PennCnvSeq.class);
//                secondarySortJob.setNumReduceTasks(24);
                secondarySortJob.setNumReduceTasks(numRecudeTasks);
                secondarySortJob.setInputFormatClass(TextInputFormat.class);
                secondarySortJob.setMapperClass(BinSortMapper.class);
                secondarySortJob.setMapOutputKeyClass(RefBinKey.class);
                secondarySortJob.setMapOutputValueClass(Text.class);
                secondarySortJob.setPartitionerClass(ChrPartitioner.class);
                secondarySortJob.setGroupingComparatorClass(ChrGroupingComparator.class);
                secondarySortJob.setReducerClass(CnvReducer.class);
                secondarySortJob.setOutputKeyClass(Text.class);
                secondarySortJob.setOutputValueClass(Text.class);
//                FileInputFormat.addInputPath(secondarySortJob, new Path("workdir/bins/"));
//                fileSystem.delete(new Path("workdir/cnv"), true);
//                FileOutputFormat.setOutputPath(secondarySortJob, new Path("workdir/cnv"));
                FileInputFormat.addInputPath(secondarySortJob, new Path(binsFolder));
                fileSystem.delete(new Path(cnvFolder), true);
                FileOutputFormat.setOutputPath(secondarySortJob, new Path(cnvFolder));
                System.out.println("Submitting CNV caller.");
                if (!secondarySortJob.waitForCompletion(true)) {
                    System.out.println("CNV caller failed.");
                    return -1;
                }

                if (debug) {
                    System.out.println("Done of submitting CNV caller.");
                }
            }
        } catch (IOException | IllegalArgumentException | IllegalStateException | InterruptedException | ClassNotFoundException ex) {
            System.err.println("Error in run, caused by " + ex.toString());
            return -1;
        }
        return 0;
    }

    public static void main(String[] args) {
        try {
            System.out.println("It is running... Please wait...");
            startTime = System.currentTimeMillis();
//            args = new String[1];
//            args[0] = "example/config.txt";
////            System.out.println("args_length: " + args.length);
            Configuration conf = new Configuration();
            int res = ToolRunner.run(conf, new PennCnvSeq(), args);
            //PennCnvSeq pennCnvSeq = new PennCnvSeq(parser.getConfiguration(),args);
            //GenericOptionsParser parser = new GenericOptionsParser(args);
            //PennCnvSeq pennCnvSeq = new PennCnvSeq(args);
            //pennCnvSeq.run();
            System.out.println("Pipeline is complete.");
            endTime = System.currentTimeMillis();
            String runningTime = "Running Time: " + Math.rint((endTime - startTime) * 100) / 100000.0 + " seconds.\n";
            System.out.println(runningTime + "\n\n");
        } catch (Exception ex) {
            System.err.println("Error in main of PennCnvSeq, caused by " + ex.toString());
            System.exit(1);
        }
    }
}

class ChrGroupingComparator extends WritableComparator {

    protected ChrGroupingComparator() {
        super(RefBinKey.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        RefBinKey key1 = (RefBinKey) w1;
        RefBinKey key2 = (RefBinKey) w2;
        return key1.getRefName().compareTo(key2.getRefName());
    }
}

class ChrPartitioner extends Partitioner<RefBinKey, Text> {

    /*
     * Delegates the refname of the composite key to the hash partitioner
     */
    private final HashPartitioner<Text, Text> hashPartitioner = new HashPartitioner<>();
    private final Text newKey = new Text();

    @Override
    public int getPartition(RefBinKey key, Text value, int numReduceTasks) {
        try {
            newKey.set(key.getRefName());
            return hashPartitioner.getPartition(newKey, value, numReduceTasks);
        } catch (Exception ex) {
            System.err.println("Error in ChrPartitioner, caused by " + ex.toString());
            return (int) (Math.random() * numReduceTasks);
        }
    }
}
