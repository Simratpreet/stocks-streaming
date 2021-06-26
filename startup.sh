kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic stock_tweets_test

kafka-console-consumer --bootstrap-server localhost:9092 --topic stock_tweets_test --from-beginning