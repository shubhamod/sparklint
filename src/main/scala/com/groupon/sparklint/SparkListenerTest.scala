package com.groupon.sparklint

import org.apache.spark.scheduler._
import org.apache.spark.{SparkConf, SparkFirehoseListener}


class SparkListenerTest extends SparkListener {

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    println("Application name: " + applicationStart.appName)
    println("Application ID: " + applicationStart.appId)
    println("Application Start Time: " + applicationStart.time)
    println("Application User: " + applicationStart.sparkUser)
    println("Application AttemptId: " + applicationStart.appAttemptId)
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    println("Application End Time: " + applicationEnd.time)
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    println("Job Start ID: " + jobStart.jobId)
    println("Job Start Time: " + jobStart.time)
    println("Job properties: " + jobStart.properties)
    println("Job Stage IDS List: " + jobStart.stageIds)
    println("Job Stag Info: " + jobStart.stageInfos)
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    println("Job End ID: " + jobEnd.jobId)
    println("Job End Result: " + jobEnd.jobResult)
    println("Job End time: " + jobEnd.time)
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    println("Stage Start properties: " + stageSubmitted.properties)
    println("Stage Start INFO: " + stageSubmitted.stageInfo)
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    println("Stage Completed INFO: " + stageCompleted.stageInfo)
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    println("Task StageID INFO: " + taskStart.stageId)
    println("Task INFO: " + taskStart.taskInfo)
    println("Task StageAttemptID INFO: " + taskStart.stageAttemptId)
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    println("Task StageID INFO: " + taskEnd.stageId)
    println("Task INFO: " + taskEnd.taskInfo)
    println("Task StageAttemptID INFO: " + taskEnd.stageAttemptId)
    println("Task Reason INFO: " + taskEnd.reason)
    println("Task TaskType INFO: " + taskEnd.taskType)
    println("Task InputMetrics INFO: " + taskEnd.taskMetrics.inputMetrics)
    println("Task outputMetrics INFO: " + taskEnd.taskMetrics.outputMetrics)
  }

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = {
    println("OnTaskGettingResult  INFO" + taskGettingResult.taskInfo)
  }

}
