## Terminology

YARN handles management of resources like memory and CPU for the various slave nodes. The master node is called the resourcemanager, the slaves are nodemanagers.

HDFS is the other aspect that handles the distribution of data. The master node(s) is called the namenode/secondarynamenode and the slaves nodes are datanodes.

## INITIALIZING HADOOP UNDER USERNAME hadoop *

Configure which nodes are the slaves by editing $HADOOP_CONF_DIR/slaves

You'll want to first format the HDFS system from the namenode. Command is:

```
hdfs namenode -format
```

Launch HDFS with:

```
start-dfs.sh
```

Then you'll want to make a root directory to store all user home directories as such:

```
hdfs dfs -mkdir /user
```

Then for each username (which matches the unix username), apply the command:

```
hdfs dfs -mkdir/<username>
```

Make sure that new directory is writable by the user:

```
hdfs dfs -chown -R <username>:superuser /user/<username>/
```

Run a test job under username hadoop, and then run:

```
hdfs dfs -chmod -R 777 /tmp
```



