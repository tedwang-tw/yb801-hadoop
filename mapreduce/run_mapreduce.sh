#---------------------------------------------
#	Removing dir
hdfs dfs -rmr output/classification-tfidf
hdfs dfs -rmr output/transposition
hdfs dfs -rmr output/similarityETL
#---------------------------------------------
#	Doing ClassificationTFIDF
hadoop jar ClassificationTFIDF.jar ClassificationTFIDF input/tfidf-input-data/jobword_merge_test output/classification-tfidf
#---------------------------------------------
#	Doing Transposition
#hadoop jar Transposition.jar TranspositionMR output/classification-tfidf/part-r-00000 output/transposition
#---------------------------------------------
#	Doing SimilarityETL
hadoop jar SimilarityETL.jar SimilarityETLMR output/classification-tfidf/part-r-00000 output/similarityETL
#---------------------------------------------
#	CopyToLocal
hdfs dfs -copyToLocal output/similarityETL/part-r-00000 /home/cloudera/big-data-analysis/output/beforeSimilarity
#---------------------------------------------
#hdfs dfs -copyToLocal output/classification-tfidf/part-r-00000 /home/cloudera/big-data-analysis/output/testfile
