package com.ibeifeng.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 基于Scala的贷出模式 SparkCore编程模板
 *      贷出函数
 *      用户函数
 */
object LoanPartternSpark {

  /**
   * 1, MAIN 函数
   */
  def main(args: Array[String]) {
    // 调用 贷出函数即可
    sparkOperation(args)(processData)
  }

  /**
   * 2, 贷出函数
   *    调用用户函数即可
   */
  def sparkOperation(args: Array[String])(operation: SparkContext => Unit): Unit ={

    // invalidate args
    if(args.length != 2){
      println("Usage <application name> <master>")
      throw new IllegalArgumentException("need tow args")
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
    // val sc = new SparkContext(sparkConf)
    val sc = SparkContext.getOrCreate(sparkConf)

    try{
      // 此处调用用户函数
      operation(sc)
    }finally {
      // SparkContext Stop
      sc.stop()
    }

  }

  /**
   * 3, 用户函数
   *    user function
   *    定义了用户自己希望之星的操作（这些操作需要使用系统资源）
   */
  def processData(sc: SparkContext): Unit ={
    // TODO 用户所编写的代码


  }

}
