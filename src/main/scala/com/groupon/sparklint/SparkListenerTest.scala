package com.groupon.sparklint

import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationStart, SparkListenerEvent}
import org.apache.spark.{SparkConf, SparkFirehoseListener}

class SparkListenerTest extends SparkListener {

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    println("Application name: " + applicationStart.appName)
    println("Application ID: " + applicationStart.appId)
    println("Application time: " + applicationStart.time)
    println("Application User: " + applicationStart.sparkUser)
  }

}
