#!/bin/bash
if [ ! -e ../bin ]; then
    mkdir ../bin
fi
#-----------------------
#ClusterTFIDF
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop/lib/*:/usr/lib/hadoop-mapreduce/*:/usr/lib/hadoop-mapreduce/lib/*:/usr/lib/hadoop-hdfs/*:/usr/lib/hadoop-hdfs/lib/*:/usr/lib/mahout/*: -d ../bin ClusterTFIDF.java SimilarityETL.java KmeansETL.java CreateSequenceFile.java CleanKmeansOutputData.java OneDocTFIDF.java FileSystemCat.java 
#javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop/lib/*:/usr/lib/hadoop-mapreduce/*:/usr/lib/hadoop-mapreduce/lib/*:/usr/lib/hadoop-hdfs/*:/usr/lib/hadoop-hdfs/lib/*:/usr/lib/mahout/*: -d ../bin CreateSequenceFile.java
#-------------------
#Similarity
#javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop/lib/*:/usr/lib/hadoop-mapreduce/*:/usr/lib/hadoop-mapreduce/lib/*:/usr/lib/hadoop-hdfs/*:/usr/lib/hadoop-hdfs/lib/*: -d ../bin Similarity.java
#-------------------
#	Making .jar
if [[ $? -eq 0 ]]; then
    jar -cvf ../Cluster.jar -C ../bin/ .
fi
