for i in {1..32}
do
    ssh compute-0-$i rm -rf /tmp/hadoop/dfs/data
    ssh compute-0-$i rm -rf /tmp/hadoop/dfs/name
    ssh compute-0-$i pkill -u $USER
done
