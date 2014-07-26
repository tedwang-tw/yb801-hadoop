#---------------------------------------------
#Copying data from local 
hdfs dfs -rmr input/new_Doc.txt
hdfs dfs -D dfs.blocksize=1048576 -put /home/cloudera/Downloads/new_Doc.txt input
#---------------------------------------------
#Setting hadoop
export HADOOP_OPTS="$HADOOP_OPTS -Xmx4G"
#---------------------------------------------
#Doing Cluster TFIDF
hdfs dfs -rmr output/one-cluster-tfidf
hadoop jar Cluster.jar OneDocTFIDF input/new_Doc.txt output/one-cluster-tfidf
#---------------------------------------------
#Doing Similarity
#hdfs dfs -rmr output/similarityETL
#hdfs dfs -rmr output/similarity
#hadoop jar Cluster.jar SimilarityETL output/one-cluster-tfidf/part-r-00000 output/one-similarityETL
#hadoop jar Similarity.jar DXD_test output/one-similarityETL/part-r-00000 output/one-dxd
#hdfs dfs -copyToLocal output/one-dxd/part-r-00000 /home/cloudera/big-data-analysis/output/one-dxd
#---------------------------------------------
#Doing KmeansETL
#hdfs dfs -rmr output/kmeansETL
#hadoop jar Cluster.jar KmeansETL output/one-doc-tfidf/part-r-00000 output/one-kmeansETL
#hdfs dfs -copyToLocal output/one-kmeansETL/part-r-00000 /home/cloudera/big-data-analysis/output/one-forCreateSequenceFile
#jar ClusterTFIDF.jar CreateSequenceFile output/forKmeans output/


#hdfs dfs -copyToLocal output/kmeansETL/part-r-00000 /home/cloudera/big-data-analysis/output/jobs


#---------------------------------------------
#hdfs dfs -copyToLocal output/classification-tfidf/part-r-00000 /home/cloudera/big-data-analysis/output/tfidf

#hdfs dfs -copyToLocal output/transposition/part-r-00000 /home/cloudera/big-data-analysis/output/tfidf-transposition

#---------------------------------------------
#To run the K-Means cluster
#mahout kmeans -i jobsSequenceFile -c output/jobs-metadata-output -o output/jobs-Kmeans-cluster -dm org.apache.mahout.common.distance.CosineDistanceMeasure -cd 0.5 -k 20 -x 20 -cl 

#---------------------------------------------
#mahout clusterdump -i output/jobs-Kmeans-cluster/clusters-20-final -o final-20iteration -dt sequencefile
#mahout seqdumper -i output/jobs-Kmeans-cluster/clusteredPoints > points-20iteration
