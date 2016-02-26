# kafka-spark-es
This program using spark streaming to fetch tweets does the following,

1. Detect language
2. Determine sentiment score
3. Create ElasticSearch index

#Start Zookeeper
zookeeper-server-start config/zookeeper.propertiers

#Start Kafka Server
kafka-server-start config/server.properties

#Create topic name: tweets
kafka-topics --zookeeper localhost:2181 --topic --create tweets2 --partitions 1 --replication-factor 1 

#Verify topic exists:
kafka-topics --zookeeper localhost:2181 --describe --topic tweets2


#Running Kafka producer:
spark-submit  ---class com.esri.kafka.KafkaProducer target\jar-file-with-dependencies


#We will use Avro to serialze/deserialize the tweets
'''
{
	"namespace": "com.esri.avro",
	"type": "record",
	"name": "Tweet",
	"fields": [
		{"name": "name", "type": "string"},
		{"name": "text", "type": "string", "avro.java.string": "String" },
		{"name": "language", "type": "string", "avro.java.string": "String" },
		{"name": "score", "type": "int", "default" : 0 }		
	]
}
'''

#Verify the tweets using kafka console consumer
kafka-console-consumer --zookeeper localhost:2181 --topic tweets2

#Start elasticsearch
bin\elasticsearch

#go to marvel sense UI, http://localhost:9200/_plugin/marvel/sense/
#Create usertweets index using marvel
'''
POST user_tweets
{
  "settings": {
    "index": {
      "number_of_shards": 5
    }
  },
  "mappings": {
    "sentiments": {
      "properties": {
        "language": {	
          "type": "string"
        },
        "score": {
          "type": "integer"
        },
        "text": {
          "type": "string"
        },
        "name": {
          "type": "string"
        }
      }
    }
  }
}
'''

#Running Kafka consumer to populate search index with scored tweets
spark-submit  ---class com.esri.kafka.KafkaConsumer target\jar-file-with-dependencies