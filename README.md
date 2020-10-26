# kafkaapp-test

- Download kafak package 

-- For windows :

  1 - Run Zookeeper Server : start bin\windows>start zookeeper-server-start.bat config\zookeeper.properties 
  2 - Run Kafka Server : start start bin\windows\kafka-server-start.bat config\server.properties
  3 - Create topics : bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 partitions 1 --topic topic_name
  4 - Run the consumer : bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic topic_name --from-beginning
  5 - Run the producer : bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic topic_name
  
  
