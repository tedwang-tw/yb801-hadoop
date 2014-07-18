#!/bin/bash

if [ ! -e jardir ]; then
    mkdir jardir
fi

#-----------------------
#	ClassificationTFIDF

#javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop/lib/*:/usr/lib/hadoop-mapreduce/*:/usr/lib/hadoop-mapreduce/lib/*:/usr/lib/hadoop-hdfs/*:/usr/lib/hadoop-hdfs/lib/*: -d jardir ClassificationTFIDF.java ClassificationTFIDFMapper.java ClassificationTFIDFReducer.java MRTools.java

#-------------
#	KmeansETL

#javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop/lib/*:/usr/lib/hadoop-mapreduce/*:/usr/lib/hadoop-mapreduce/lib/*:/usr/lib/hadoop-hdfs/*:/usr/lib/hadoop-hdfs/lib/*: -d jardir KmeansETL.java KmeansETLMapper.java KmeansETLPartitioner.java KmeansETLReducer.java

#-------------------
#	TranspositionMR

#javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop/lib/*:/usr/lib/hadoop-mapreduce/*:/usr/lib/hadoop-mapreduce/lib/*:/usr/lib/hadoop-hdfs/*:/usr/lib/hadoop-hdfs/lib/*: -d jardir TranspositionMR.java

#-------------------
#	SimilarityETLMR

javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop/lib/*:/usr/lib/hadoop-mapreduce/*:/usr/lib/hadoop-mapreduce/lib/*:/usr/lib/hadoop-hdfs/*:/usr/lib/hadoop-hdfs/lib/*: -d jardir SimilarityETLMR.java

#---------------
#	Making .jar

if [[ $? -eq 0 ]]; then
    jar -cvf SimilarityETL.jar -C jardir/ .
#    jar -cvf Transposition.jar -C jardir/ .
#    jar -cvf KmeansETL.jar -C jardir/ .
#    jar -cvf ClassificationTFIDF.jar -C jardir/ .
fi
