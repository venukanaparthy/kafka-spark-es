package com.esri.kafka

import com.typesafe.config.ConfigFactory
import kafka.serializer.{DefaultDecoder, StringDecoder}
import com.twitter.bijection.avro.SpecificAvroCodecs
import com.esri.avro.Tweet
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.elasticsearch.spark.rdd.EsSpark
import com.esri.spark.SentimentAnalyzer

object KafkaConsumer {

  private val conf = ConfigFactory.load()

   def main (args: Array[String]) {
      val sparkConf = new SparkConf().setAppName("kafka-twitter-consumer").setMaster("local[*]")
      sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      sparkConf.registerKryoClasses(Array(classOf[Tweet]))
      val sc = new StreamingContext(sparkConf, Seconds(1))
         
      val sa:SentimentAnalyzer = new SentimentAnalyzer();
      sa.init();
      val saBC = sc.sparkContext.broadcast[SentimentAnalyzer](sa)
      val encTweets = {
        val topics = Map(KafkaProducer.KafkaTopic -> 1)
        val kafkaParams = Map(
          "zookeeper.connect" -> conf.getString("kafka.zookeeper.quorum"),
           "metadata.broker.list" -> conf.getString("kafka.brokers"),
          "group.id" -> "1")
         KafkaUtils.createStream[String, Array[Byte], StringDecoder, DefaultDecoder](
          sc, kafkaParams, topics, StorageLevel.MEMORY_ONLY)
          //KafkaUtils.createDirectStream(sc, kafkaParams, Set(KafkaProducerApp.KafkaTopic))
      }
            
              
      val tweets = encTweets.flatMap(x => SpecificAvroCodecs.toBinary[Tweet].invert(x._2).toOption)      
      val tweetsScore =  tweets.map { x =>                
                   new Tweet(x.getName, x.getText, saBC.value.detectLanguage(x.getText), saBC.value.findSentiment(x.getText))                  
                  }
          
      tweetsScore.foreachRDD{ (tweets:RDD[Tweet], batchTime: Time) => {
          tweets.collect().foreach(println)                 
          EsSpark.saveJsonToEs(tweets, "user_tweets/sentiments")
          }
         }
         
      
      //tweetsScore.print();
    
      sc.start()
      sc.awaitTermination()
  }
}
