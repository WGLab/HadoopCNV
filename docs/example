## Get Hadoop set up

Maven is the preferred compilation tool for Hadoop, Hadoop-BAM, and PennCNV3. Please visit https://maven.apache.org/ if you do not already have Maven installed.

You will want to install Hadoop on your cluster if you have not done so already. More information can be found here:

https://hadoop.apache.org/

Our software relies upon an open source package called Hadoop BAM to parse the BAM binary files and split these for our Mappers. Hadoop BAM can be obtained from

http://sourceforge.net/projects/hadoop-bam/

Please visit the initialization.md page for information on how to configure Hadoop.

## Download and build the project

Go to a directory where you want the PennCNV3 project to reside in. You can pull the code from Git using instructions on this site. Change to the new directory PennCNV3. This directory will be referred to as the project root directory (PROJECT_ROOT). In PROJECT_ROOT, open up pom.xml to make any necessary edits.  For example, in the XML tag hadoop.version, you may change this value if you are running a newer version of Hadoop.

To build the project, simply type "mvn package", and source code will be generated in PROJECT_ROOT/target. You can also optionally generate HTML documentation for the various Java source files, and these will be generated in PROJECT_ROOT/target/site/apidocs with an invocation of "mvn javadoc:javadoc".

## Download small dataset and configure PennCNV3 to use this data

Let's get started with a small example dataset available from the 1000 Genomes project, namely Chr 22 data of the sample ID NA12878. What we will need is two files from the project. We will download these into PROJECT_ROOT/examples. The VCF file can be found at ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/release/20110521/ALL.chr22.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf.gz and the BAM file can be found at (KAI: Can you please post a link here? I can't find it on KGP site).

Go to the directory that you saved the BAM and VCF files. Upload the BAM file into HDFS. This can be done with the command "hdfs dfs -put <BAMFILE> /bamfiles/". Move the VCF file into the directory PROJECT_ROOT/examples/vcfs/. Change to the PROJECT_ROOT/example directory, and open up config.txt for editing. Be sure the value for the BAM_FILE and VCF_FILE reflect the correct value for the names of the files you just downloaded.

# Run the example dataset

In config.txt file, you will want to ensure that the values for RUN_DEPTH_CALLER,INIT_VCF_LOOKUP,RUN_BINNER,and RUN_CNV_CALLER are set to true. You can leave RUN_GLOBAL_SORT set to false. This last component basically sorts the output from RUN_DEPTH_CALLER, and is helpful for debugging purposes.

You should be ready to run this example. But before we do let's make sure the HADOOP_CLASSPATH is set correctly. You will wanto to make sure HADOOP_CLASSPATH is pointing to the path of the JAR file from the Hadoop BAM installation. My command for example in the Bash initialization script reads "export HADOOP_CLASSPATH=/home/hadoop/hadoop/Hadoop-BAM-master/target/hadoop-bam-7.0.1-SNAPSHOT-jar-with-dependencies.jar". You may want to add this in your initialization script as well, logout and login. Verify the variable is set correctly with a call to "echo $HADOOP_CLASSPATH".

At this point, you can run PennCNV3 with a call to "./run.sh"




