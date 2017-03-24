package com.ibeifeng.bigdata.spark

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 基于Scala的贷出模式 SparkCore编程模板
 *      贷出函数
 *      用户函数
 */
object WriteHBaseSpark {

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

    // 模拟数据
    val list = List(("hadoop", "234"), ("spark", "200"), ("hive", "156"))

    // Create RDD
    val rdd: RDD[(String, String)] = sc.parallelize(list)

    /**
     * 将RDD的数据持久化到HBase表中
     *    1, 需要将RDD的数据转换为(key,value)的格式，符合Reduce将数据插入HBase表中的格式
     */
    val keyValueRdd: RDD[(ImmutableBytesWritable, Put)] = rdd.map(tuple => {
      // Create Put Instance
      val put = new Put(Bytes.toBytes(tuple._1))   // ROWKEY

      // add columnFamily
      put.add(
        Bytes.toBytes("info"),
        Bytes.toBytes("count"),
        Bytes.toBytes(tuple._2)
      )

      // return
      (new ImmutableBytesWritable(Bytes.toBytes(tuple._1)), put)
    })

    /**
     * 获取配置信息，并保存数据的值HBase表
     */
    val conf = HBaseConfiguration.create()

    // SET OUTPUT FORMAT
    conf.set("mapreduce.job.outputformat.class",
        "org.apache.hadoop.hbase.mapreduce.TableOutputFormat")
    // SET OUTPUT TABLE
    conf.set(TableOutputFormat.OUTPUT_TABLE, "jimmy_wordcount")
    // SET OUPUT DIR
    conf.set("mapreduce.output.fileoutputformat.outputdir",
      "/hbase/write-count/11111")

    // 保存
    keyValueRdd.saveAsNewAPIHadoopDataset(conf)
  }

}
