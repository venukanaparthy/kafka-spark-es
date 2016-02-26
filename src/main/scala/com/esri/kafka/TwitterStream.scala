package com.esri.kafka

import com.typesafe.config.ConfigFactory
import twitter4j._
import twitter4j.conf.{ConfigurationBuilder, Configuration}
import twitter4j.auth.Authorization;
import twitter4j.auth.AuthorizationFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationContext;

object TwitterStream {

  private val conf = ConfigFactory.load()

  private val getTwitterConf: Configuration = {
    var twitterConf:Configuration = ConfigurationContext.getInstance();    
    twitterConf
  }


  def getStream = new TwitterStreamFactory(getTwitterConf).getInstance()

  class OnTweetPosted(cb: Status => Unit) extends StatusListener {

    override def onStatus(status: Status): Unit = cb(status)
    override def onException(ex: Exception): Unit = throw ex

    override def onStallWarning(warning: StallWarning): Unit = {}
    override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = {}
    override def onScrubGeo(userId: Long, upToStatusId: Long): Unit = {}
    override def onTrackLimitationNotice(numberOfLimitedStatuses: Int): Unit = {}
  }


}
