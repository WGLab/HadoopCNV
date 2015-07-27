#!/bin/bash

hadoop jar ../target/penncnv-seq-0.1.jar edu.usc.PennCnvSeq -libjars $HADOOP_CLASSPATH config.txt
#hadoop jar ../target/penncnv-seq-0.1.jar edu.usc.PennCnvSeq -libjars $HADOOP_BAM/target/hadoop-bam-7.0.1-SNAPSHOT-jar-with-dependencies.jar config.txt
hdfs dfs -getmerge workdir/cnv/$BAM_FILENAME/* results.txt
