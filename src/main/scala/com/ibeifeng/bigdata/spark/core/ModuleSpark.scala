package com.ibeifeng.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 基于SparkCore实现[网站流量PV和UV统计]
 */
object ModuleSpark {

  /**
   * Driver Program
   *    JVM Process -  Main 运行的Process
   *
   * @param args
   */
  def main(args: Array[String]) {

    // 读取Spark Application 的配置信息
    val sparkConf = new SparkConf()
      // 设置应用的名称
      .setAppName("TrackLogAnalyzerSpark Application")
      // 设置程序的运行环境, local
      .setMaster("local[2]")
    // 创建Spark上下文对象
    val sc = new SparkContext(sparkConf)

    // =====================================================================
    /**
     * Step 1:  read data
     *    SparkContext用于读取数据
     */


    /**
     * Step 2: process data
     *    RDD#transformation
     */


    /**
     * Step 3: write data
     *    将处理的结果数据存储
     *    RDD#action
     */


    // =====================================================================
    sc.stop()  // 关闭资源
  }

}
