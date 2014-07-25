#!/bin/bash
#---------------------------------------------
#Copying data from local 
hdfs dfs -rmr input/jobword_merge.txt
hdfs dfs -rmr input/keywords_merge_sort_index.txt
hdfs dfs -D dfs.blocksize=1048576 -put input/jobword_merge.txt input
hdfs dfs -D dfs.blocksize=1048576 -put input/keywords_merge_sort_index.txt input

#---------------------------------------------
#Setting hadoop
export HADOOP_OPTS="$HADOOP_OPTS -Xmx4G"

#---------------------------------------------
#Doing Cluster TFIDF
hdfs dfs -rmr output/cluster-tfidf
hadoop jar Cluster.jar ClusterTFIDF input/jobword_merge.txt output/cluster-tfidf

#---------------------------------------------
#Doing Similarity
#hdfs dfs -rmr output/similarityETL
#hdfs dfs -rmr output/similarity
#hadoop jar Cluster.jar SimilarityETL output/cluster-tfidf/part-r-00000 output/similarityETL
#hadoop jar Similarity.jar DXD_test output/similarityETL/part-r-00000 output/dxd
#hdfs dfs -copyToLocal output/similarityETL/part-r-00000 output/forSimilarity

#---------------------------------------------
#Doing KmeansETL
#hdfs dfs -rmr output/kmeansETL
#hadoop jar Cluster.jar KmeansETL output/cluster-tfidf/part-r-00000 output/kmeansETL
#hdfs dfs -copyToLocal output/kmeansETL/part-r-00000 output/forCreateSequenceFile

#Create sequence file
#java -jar CreateSequenceFile.jar output/forCreateSequenceFile output/jobsSequenceFile
#hdfs dfs -rmr input/jobsSequenceFile
#hdfs dfs -D dfs.blocksize=1048576 -put output/jobsSequenceFile input

#Doing mahout k-means cluster
#mahout kmeans -i input/jobsSequenceFile -c output/jobs-kmeans-metadata -o output/jobs-kmeans-cluster -dm org.apache.mahout.common.distance.CosineDistanceMeasure -cd 0 -k 30 -x 100 -cl 

#Dumping output data
#if [ -e output/final ]; then
#    rm -f output/final
#fi

#mahout clusterdump -i output/jobs-kmeans-cluster/clusters-100-final -o output/final -dt sequencefile

#if [ -e output/points ]; then
#    rm -f output/points
#fi

#mahout seqdumper -i output/jobs-kmeans-cluster/clusteredPoints > output/points

#if [ -e output/kmeans.txt ]; then
#    rm -f output/kmeans.txt
#fi

#java -jar CleanKmeansOutputData.jar output/points output/kmeans.txt

