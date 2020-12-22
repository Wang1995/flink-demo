

# 


flink run -m yarn-cluster  \
-yn 2 /usr/lib/flink/examples/streaming/WordCount.jar  \
--input s3://chengang-test/data/word.txt  \
--output s3://chengang-test/data/input.txt

# save S3 use s3a
# conf value  s3.endpoint 
flink run -m yarn-cluster -c wh.tech.batch.TestJava FlinkExample-1.0-SNAPSHOT-jar-with-dependencies.jar \ 
    --path s3a://chengang-test/flink/kafka_user

flink run -m yarn-cluster -c wh.tech.batch.TestScala FlinkExample-1.0-SNAPSHOT-jar-with-dependencies.jar --path s3a://chengang-test/flink/kafka_user
    
# save hdfs use hdfs
# conf value  
flink run -m yarn-cluster -c wh.tech.batch.TestJava FlinkExample-1.0-SNAPSHOT-jar-with-dependencies.jar \ 
    --path hdfs://master:8020/flink/kafka_user
    
# save local flie
# conf value  
flink run -m yarn-cluster -c wh.tech.batch.TestJava FlinkExample-1.0-SNAPSHOT-jar-with-dependencies.jar \ 
    --path /flink/kafka_user
    
    
# run python local 
flink run -m yarnpcluster -pyarch venv.zip -pyexec venv.zip/venv/bin/Python -py deploy_demo.py