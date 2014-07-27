sqoop export --connect jdbc:mysql://localhost/jobs_information --table keyword --export-dir /user/cloudera/output/keywords.txt --input-fields-terminated-by ',' --num-mappers 1 --direct
