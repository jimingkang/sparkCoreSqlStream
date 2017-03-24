package com.ibeifeng.bigdata.spark.core

/**
  * Created by jka07@int.hrs.com on 3/17/17.
  */
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * 基于SparkCore实现[网站流量PV和UV统计]
  */
object GroupSort{

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
      .setAppName("GroupSortSpark Application")
      // 设置程序的运行环境, local
      .setMaster("local[2]")
    // 创建Spark上下文对象
    val sc = new SparkContext(sparkConf)

    // =====================================================================
    /**
      * Step 1:  read data
      *    SparkContext用于读取数据
      */
    val rdd = sc.textFile("/test/group.txt")

    /**
      * Step 2: process data
      *    RDD#transformation
      *
      *    先按第一个字段进行分组，接着按第二个字段进行降序排序，获取前3个最大值
      */
    val xx = rdd.map(line => {
      // 进行分割
      val splited = line.split(" ")
      //
      (splited(0).toString, splited(1).toInt)
    }) // RDD[(String, Int)]
      .groupByKey()  // RDD[(String, Iterable[Int])]
      .map{
      case (key, iter) => {
        (key, iter.toList.sorted.takeRight(3).reverse)
      }
    } // RDD[(String, List[Int])]


    /**
      * 思考：
      *    当 分组以后，各个组中的数据量很大的话，不建议如此编程，由于排序需要消耗内存
      *
      * 如何解决呢？？？？
      *    拆
      *  思想就是：
      *      将一个任务 变成 两个任务完成
      */
    val yy = rdd.map(line => {
      val random = new Random(10)
      // 进行分割
      val splited = line.split(" ")
      //
      (splited(0) + "_" + random, splited(1).toInt)
    }) // RDD[(String, Int)]
      .groupByKey()
      .map{
        case (key, iter) => {
          (key.split("_")(0), iter.toList.sorted.takeRight(3).reverse)
        }
      }.groupByKey()/*.map{
      case (key, iter) => {
        (key, iter.toList.sorted.takeRight(3).reverse)
      }
    }*/

yy.foreach(iter=>{
  println(iter._1 +":"+iter._2)
})


    /**
      * Step 3: write data
      *    将处理的结果数据存储
      *    RDD#action
      */


    // =====================================================================
    sc.stop()  // 关闭资源
  }

}

