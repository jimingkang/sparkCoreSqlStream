package com.ibeifeng.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 基于SCALA中的贷出模式, 编写Spark程序的编程模块
 *    -1,贷出函数
 *    -2,用户函数
 *    一般情况下,将贷出函数写在main 函数之前, 用户函数写在main 函数之后
 */
object LoanPartternSpark {

  /**
   * 贷出模式中的
   *    贷出函数
   * @param args
   */
  def sparkOperation(args: Array[String])(operation: SparkContext => Unit ): Unit ={
    // invalidate args
    if(args.length != 2){
      println("Uage: <application name> <master>")
      throw new IllegalArgumentException("need two args...................")
    }

    // Create SparkConf Instance
    val sparkConf = new SparkConf()
      // 设置应用的名称, 4040监控页面显示
      .setAppName(args(0))
      // 设置程序运行环境, local
      .setMaster(args(1))

    // Create SparkContext
    /**
     * -1,读取数据，创建RDD
     * -2,负责调度Job和监控
     */
    val sc = new SparkContext(sparkConf)

    /**
     * process data
     */
    try{
      // 调用 用户函数
      operation(sc)
    }finally {
      // SparkContext Stop
      sc.stop()
    }
  }

   /**
    * Spark Application  Running Entry
    *     Driver Program
    * @param args
    */
   def main(args: Array[String]) {
     // 调用贷出函数即可,传递用户函数
     sparkOperation(args)(processData)
   }

  /**
   * 贷出模式中的
   *    用户函数
   * 对于Spark 程序应用来说
   *     -1, 读取文件内容
   *     -2, 处理数据
   *     -3, 结果输出
   *
   * @param sc
   */
  def processData(sc: SparkContext): Unit ={
    /**
     * Step 1: input data -> RDD
     */


    /**
     * Step 2: process data -> RDD#transformation
     */


    /**
     * Step 3: output data -> RDD#action
     */
  }
 
 }
