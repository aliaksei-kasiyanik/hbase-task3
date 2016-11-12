
nl graph1.tsv > graph1-hbase.tsv

hdfs dfs -mkdir /user/cloudera/akasiyanik-data
hdfs dfs -put graph1-hbase.tsv /user/cloudera/akasiyanik-data

rm graph1-hbase.tsv

hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.columns="HBASE_ROW_KEY,s,t,w" -Dimporttsv.bulk.output="/user/cloudera/akasiyanik-data/storefile1" kasiyanik-graph-1 /user/cloudera/akasiyanik-data/graph1-hbase.tsv
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles /user/cloudera/akasiyanik-data/storefile1 kasiyanik-graph-1

hbase org.apache.hadoop.hbase.mapreduce.RowCounter kasiyanik-graph-1

hdfs dfs -rm -r /user/cloudera/akasiyanik-data