package com.ibeifeng.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  */
object AccessAnalyzerSpark {
 
   /**
    * Spark Application  Running Entry
    *
    * Driver Program
    * @param args
    */
   def main(args: Array[String]) {
     // Create SparkConf Instance
     val sparkConf = new SparkConf()
         // 设置应用的名称, 4040监控页面显示
         .setAppName("LogAnalyzerSpark Application")
         // 设置程序运行环境, local
         .setMaster("local[2]")
 
     // Create SparkContext
     /**
      * -1,读取数据，创建RDD
      * -2,负责调度Job和监控
      */
     val sc = new SparkContext(sparkConf)
 /**  ======================================================================= */
     /**
      * Step 1: input data -> RDD
      */
      val logFile = "/test/access_log"

      val accessLogsRdd: RDD[ApacheAccessLog] = sc
          .textFile(logFile)   // read file from hdfs
          // filter
          .filter(ApacheAccessLog.isValidateLogLine)
          // parse
          .map(log => ApacheAccessLog.parseLogLine(log))
      // cache data
      accessLogsRdd.cache()
      println("Count:" + accessLogsRdd.count())

     /**
      * Step 2: process data -> RDD#transformation
      */
     /**
      * 需求一：
	      The average, min, and max content size of responses returned from the server.
      */
     val contentSizeRdd: RDD[Long] = accessLogsRdd.map(log => log.contentSize)
     // cache
     contentSizeRdd.cache()

     // compute
     val avgContentSize = contentSizeRdd.reduce(_ + _) / contentSizeRdd.count()
     val minContentSize = contentSizeRdd.min()
     val maxContentSize = contentSizeRdd.max()

     contentSizeRdd.unpersist()
     // println
     println("Content Size Avg: %s, Min: %s, Max: %s".format(
      avgContentSize, minContentSize, maxContentSize
     ))

     /**
      * 需求二：
	        A count of response code's returned.
      */
     val responseCodeToCount: Array[(Int, Int)] = accessLogsRdd
        .map(log => (log.responseCode, 1))   // WordCount
        .reduceByKey(_ + _)
        .take(5)

    println(
    s"""Response Code Count: ${responseCodeToCount.mkString("[",", ","]")}"""
    )

     /**
      * 需求三：
	        All IPAddresses that have accessed this server more than N times.
      */
     val ipAddresses: Array[(String, Int)] =  accessLogsRdd
        .map(log => (log.ipAddress, 1))
        .reduceByKey(_ + _)
        .filter(tuple => tuple._2 >= 20)
        .take(10)
     println(
       s"""IP Addresses: ${ipAddresses.mkString("[",", ","]")}"""
     )


     /**
      * 需求四：
	        The top endpoints requested by count.
      */
     val topEndpoints: Array[(String, Int)] = accessLogsRdd
        .map(log => (log.endpoint, 1))
        .reduceByKey(_ + _)
        .top(5)(OrderingUtils.SecondValueOrdering)
     /**
        .map(tuple => (tuple._2, tuple._1))
        .sortByKey(false)
        .take(5)
        .map(tuple => (tuple._2, tuple._1))
     */
     println(
       s"""Top Endpoints: ${topEndpoints.mkString("[",", ","]")}"""
     )

     accessLogsRdd.unpersist()


     /**
      * 为了调试，需要查看监控见面
      */
     Thread.sleep(1000000)



 /**  ======================================================================= */
     // SparkContext Stop
     sc.stop()
 
   }
 
 }
