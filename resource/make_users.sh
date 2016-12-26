USERS=('huiyang' 'hadoop')
hdfs dfs -mkdir /home
for user in "${USERS[@]}"
do
    hdfs dfs -mkdir /home/$user
done


