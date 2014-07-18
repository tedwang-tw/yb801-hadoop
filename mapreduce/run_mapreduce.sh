#---------------------------------------------
#Removing dir
#hdfs dfs -rmr output/classification-tfidf
#hdfs dfs -rmr output/kmeansETL
hdfs dfs -rmr output/jobs-metadata-output
hdfs dfs -rmr output/jobs-kmeans-cluster
#---------------------------------------------
#Doing ClassificationTFIDF
#hadoop jar ClassificationTFIDF.jar ClassificationTFIDF input/tfidf-input-data/jobword_merge_test output/classification-tfidf
#---------------------------------------------
#Doing Transposition
#hadoop jar Transposition.jar TranspositionMR output/classification-tfidf/part-r-00000 output/transposition
#---------------------------------------------
#Doing SimilarityETL
#hadoop jar SimilarityETL.jar SimilarityETLMR output/classification-tfidf/part-r-00000 output/similarityETL
#---------------------------------------------
#Doing KmeansETL
#hadoop jar KmeansETL.jar KmeansETLMR output/classification-tfidf/part-r-00000 output/kmeansETL
#---------------------------------------------

#hadoop jar KmeansETL.jar CreateSequenceFile hdfs://localhost.localdomain:8020/user/cloudera/output/kmeansETL/part-r-00000

#hdfs dfs -copyToLocal output/kmeansETL/part-r-00000 /home/cloudera/big-data-analysis/output/jobs


#---------------------------------------------





#---------------------------------------------
#To run the K-Means cluster
mahout kmeans -i jobsSequenceFile -c output/jobs-metadata-output -o output/jobs-Kmeans-cluster -dm org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure -cd 1.0 -k 10 -x 20 -cl -ow


#---------------------------------------------
#	CopyToLocal
#hdfs dfs -copyToLocal output/similarityETL/part-r-00000 /home/cloudera/big-data-analysis/output/beforeSimilarity
#---------------------------------------------
#hdfs dfs -copyToLocal output/classification-tfidf/part-r-00000 /home/cloudera/big-data-analysis/output/testfile

mahout clusterdump -i output/jobs-Kmeans-cluster/clusters-1-final -dt sequencefile -o /home/cloudera/big-data-analysis/output/jobsKmeans --pointsDir output/jobs-Kmeans-cluster/clusteredPoints
