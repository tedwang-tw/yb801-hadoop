#!/bin/bash
hdfs dfs -rmr mapreduce_output/dxd_output

hadoop jar dxdtest.jar DXD_test input/tfidf_index.txt mapreduce_output/dxd_output

