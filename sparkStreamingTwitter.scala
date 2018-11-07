package com.sundogsoftware.sparkstreaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import Utilities

object savetwits {
  def main (args: [String]) {
    setupTwitter()
    val ssc = new StreamingContext("local", "savetwits", Seconds(1))
    setupLogging()
    val twits = TwitterUtils.createStream(ssc, None)
    val stats = twits.map(x => x.getText()
//    stats.saveAsTextFiles("Tweets", "txt)
    var totalTweets: Long = 0
    stats.foreachRDD (rdd, time) => {
      if (rdd.count > 0 ){
        val repartitionRDD = rdd.partition(1).cache()
        repartitionRDD.saveAsTextFile("Tweets"+ time.milliseconds.toString)
        totalTweets += repartitionRDD.count()
        println("count = ", totalTweets)
        if(totalTweets > 1000) {
          System.exit(0)
        }
      }
    }
  }
  
}
