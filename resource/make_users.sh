USERS=('huiyang' 'hadoop')
hdfs dfs -mkdir /user
for user in "${USERS[@]}"
do
    hdfs dfs -mkdir /user/$user
done


