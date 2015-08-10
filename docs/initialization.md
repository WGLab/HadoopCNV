This document pertains to configuration of Hadoop as applicable to the biocluster cluster at Zilkha. It assumes the user has permissions to log in as the hadoop user.

# Terminology

YARN is the resource scheduler for the cluster. YARN is the default backbone of frameworks such as Hadoop. Other frameworks like Apache Spark can also exploit an existing YARN cluster. The master node is called the resourcemanager, the slaves are nodemanagers.

Hadoop is the other aspect that handles the distribution and processing of data. The master node(s) is called the namenode/secondarynamenode and the slaves nodes are datanodes. The distributed filesystem of Hadoop is called HDFS.

Please refer to http://zh.hortonworks.com/blog/introducing-apache-hadoop-yarn/ for more information.
# Overall Architecture

We want to assign one of the biocluster compute nodes as the head node and the remaining nodes as worker nodes. Currently, compute-0-0.local is assigned as the head node. The Hadoop and YARN scripts should always be launched from the head node. The user hadoop should run scripts in the sbin directory of the Hadoop distribution root (these are not world readable/executable). These scripts include start_dfs.sh, stop_dfs.sh, start_yarn.sh, and stop_yarn.sh, which handle starting and stopping of Hadoop and YARN services across all nodes of the cluster. Other scripts, which are found in the bin directory of the Hadoop distribution root include commands such as hdfs and yarn, which handle tasks like launching Hadoop jobs, copying files to/from HDFS, monitoring running jobs, killing jobs, etc. These are meant to be run by any user and are world readable/executable.

Please first log on as user hadoop(password:hadoop221):
```su hadoop```

Then ssh onto compute-0-0 by:
```ssh compute-0-0```
# Configuration common to all users

For each user that plans to use Hadoop, environment variables should be assigned in the user's .bash_profile initialization script.  Details follow in this section. Also, users should be able to connect over SSH to other nodes (including the master node itself) without entering a password. This should just be matter of appending the contents of ~/.ssh/id_rsa.pub (or the appropriate public key file) into ~/.ssh/authorized_keys. Verify correct behavior with a test connection over SSH.

```
export HADOOP_PREFIX=/home/hadoop/hadoop/hadoop-2.6.0-src/hadoop-dist/target/hadoop-2.6.0
export HADOOP_MAPRED_HOME=$HADOOP_PREFIX
export HADOOP_HDFS_HOME=$HADOOP_PREFIX
export YARN_HOME=$HADOOP_PREFIX
export HADOOP_CONF_DIR=$HADOOP_PREFIX/etc/hadoop

export HADOOP_BAM=/home/hadoop/hadoop/Hadoop-BAM-master
export HADOOP_CLASSPATH=$HADOOP_BAM/target/hadoop-bam-7.0.1-SNAPSHOT-jar-with-dependencies.jar
```

# Initial configuration

These steps should be completed as username hadoop on the master node. Configure which nodes are the slaves by editing $HADOOP_CONF_DIR/slaves. This file is straightforward and contains one hostname per line. Do not include the master node in this file. You will also want to identify the master node in the appropriate files. In yarn-site.xml, be sure the contents of the value tag of the yarn.resourcemanager.hostname property is set to the domain name of your master node. You will also need to set the domain name of the master node in the value tag of the fs.defaultFS property in the core-site.xml file.

You will want to add nodes into your cluster, please add the names directly into the file by:

```vi $HADOOP_CONF_DIR/slaves```

One example file is shown below:

```
compute-0-20
compute-0-21
```


You'll want to first format the HDFS system from the namenode. Command is:

WARNING: Please notice that this will delete all your data in HDFS. Please don't do this if you just want to add nodes!

WARNING: All the commands below should be used in compute-0-0 instead of HEAD node!!!!!

```
hdfs namenode -format
```

Launch HDFS with:

```
start-dfs.sh
```

We might as well launch the YARN resource manager here as well.
```
start-yarn.sh
```

Some convenience scripts are included in $HOME/hadoop directory when logged in as hadoop. The first script will initialize home directories for all Hadoop users.  Edit the line reading in make_users.sh

```
users='kaiwang huiyang garychen1'
```
to include any users that will be using the cluster. Run the script as 
```
./make_users.sh
```
A second script, test_hadoop_bam.sh will do two things. It makes sure that Hadoop-BAM is installed correctly, and it also generates some temp directories, which are necessary for regular Hadoop users to run jobs. Execute this with command
```
./test_hadoop_bam.sh
```

## Moving data to and from HDFS

Commands for filesystem operations on HDFS are almost identical to their UNIX counterparts. Make a directory with

```
hdfs dfs -mkdir <new_dir>
```

and copy files with

```
hdfs dfs -put <local path> <hdfs_path>
```
A handy command for collecting a bunch of files in a HDFS directory into a concatenated file on local storage is
```
hdfs dfs -getmerge <hdfs_dir>/* <localfile>
```

For more info type
```
hdfs dfs -help
```

## Jobs administration

The command for launching jobs is

```
hadoop jar <program_jar_file -libjars <dependency_jars> <any extra arguments>
```
Checking what jobs are running involves invoking YARN:
```
yarn application -list
```
You can kill a job with 
```
yarn application -kill <jobid>
```
 
##Debug
Bugs always occur because of the settings in XML files in $HADOOP_CONF_DIR.

To make sure everything is working without any bugs, please copy the configuration files in ./configs into $HADOOP_CONF_DIR .
 
## Web monitoring

You'll want to ensure that there is a port mapping of port TCP port 8088 to compute-0-0, so that users with a web browser can monitor progress of jobs. The URL for job monitoring is
```
http://biocluster.med.usc.edu:8088/cluster
```


