package com.ibeifeng.bigdata.spark.app

import org.apache.spark.{SparkConf, SparkContext}

/**
  */
object TestSpark {
 
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


     // old path /user/hive/warehouse/track_log/ds=20150828/hour=18
     // val trackRdd = sc.textFile("/user/hive/warehouse/track_log/ds=20150828/hour=18")

     val trackRdd = sc.textFile("/user/hive/warehouse/page_views_txt/ds=20150828/hour=18")

    //val trackRdd = sc.textFile("/user/hive/warehouse/page_views")  //page_views is externally stored in hbase ,can not be read as textFile


    // val trackRdd = sc.textFile("/user/hive/warehouse/hvtohbase")  //  hvtohbase externally stored in hbase , not working

    // val trackRdd = sc.textFile("/hbase/data/default/hvtohbase")  // this is a hbase dir( hvtohbase externally stored in hbase),  stored in hbase  not working
     // RDD Cache
     println(trackRdd.count())
     println(trackRdd.first())


 /**  ======================================================================= */
     // SparkContext Stop
     sc.stop()
   }
 
 }
