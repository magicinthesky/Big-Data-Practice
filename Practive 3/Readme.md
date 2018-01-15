## Technique: Python, Spark Streaming and Kafka


### Interpreter environment: 2.7.14(~anaconda2/bin/python)


## How to run:
> 1. Open terminal 1, install kafka and change directory to /kafka_2.11-1.0.0, and run command: 
	bin/zookeeper-server-start.sh config/zookeeper.properties
> 2. Open terminal 2, and run command: 
	bin/kafka-server-start.sh config/server.properties
> 3. Open terminal 3, and run command: 
	bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic twitteranalysis
> 4. Open terminal 4, change directory to where source codes located at and run command: 
	python twitter_app.py
> 5. Open terminal 5, and run command: 
	python streaming.py
