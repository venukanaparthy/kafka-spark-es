package com.esri.kafka

import java.util.Properties

import com.twitter.bijection.avro.SpecificAvroCodecs.{toJson, toBinary}
import com.typesafe.config.ConfigFactory
import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}
import com.esri.kafka.TwitterStream.OnTweetPosted
import com.esri.avro.Tweet
import twitter4j.{Status, FilterQuery}

object KafkaProducer {

  private val conf = ConfigFactory.load()
  
  val KafkaTopic = "tweets2"

  val kafkaProducer = {
    val props = new Properties()
    props.put("metadata.broker.list", conf.getString("kafka.brokers"))
    props.put("request.required.acks", "1")
    val config = new ProducerConfig(props)
    new Producer[String, Array[Byte]](config)
  }

  val filterUsOnly = new FilterQuery().locations(
    Array(-126.562500,30.448674),
    Array(-61.171875,44.087585))

  
  def main (args: Array[String]):Unit= {
    val twitterStream = TwitterStream.getStream
    twitterStream.addListener(new OnTweetPosted(s => sendToKafka(toTweet(s))))
    twitterStream.filter(filterUsOnly)
  }

  private def toTweet(s: Status): Tweet = {
    new Tweet(s.getUser.getName, s.getText, "none", -1)
  }

  private def sendToKafka(t:Tweet) {
    println(toJson(t.getSchema).apply(t))
    val tweetEnc = toBinary[Tweet].apply(t)
    val msg = new KeyedMessage[String, Array[Byte]](KafkaTopic, tweetEnc)
    kafkaProducer.send(msg)
  }

}



