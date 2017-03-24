package com.ibeifeng.bigdata.spark.sql

import com.ibeifeng.bigdata.spark.core.ApacheAccessLog
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by XuanYu on 2017/3/5.
 */
object SQLAccessLogAnalyzerSpark {

  def main(args: Array[String]) {

    // 读取配置信息
    val sparkConf = new SparkConf();
    sparkConf.setAppName("SQLAccessLogAnalyzer Application").setMaster("local[2]")

    // 创建SparkContext实例
    val sc = SparkContext.getOrCreate(sparkConf)

    /**
     * 创建SparkSQL入口  SQLContext
     */
    val sqlContext = new SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

// ================================================================================
    // 自定义函数
    // 案例一、定义UDF：一对一
    sqlContext.udf.register(
      "toUpper" ,// function name
      (word: String) => word.toUpperCase()
    )


    // 案例二：截取Double类型值的后几位
    sqlContext.udf.register(
      "double_scala",
      (numValue: Double, scale: Int) => {
        import java.math.BigDecimal
        val db = new BigDecimal(numValue)
        db.setScale(scale, BigDecimal.ROUND_HALF_UP).doubleValue()
      }
    )

    // 案例三：注册定义UDAF
    sqlContext.udf.register(
      "sal_avg",
      AvgSalUDAF
    )


    // ================================================================================
    val logFile = "/test/access_log"
    val accessLogsRdd: RDD[ApacheAccessLog] = sc
      .textFile(logFile)   // read file from hdfs
      // filter
      .filter(ApacheAccessLog.isValidateLogLine)
      // parse
      .map(log => ApacheAccessLog.parseLogLine(log))
    /**
     * RDD -> DataFrame
     *    其中一种方式为：RDD[CASE CLASS]
     */
    // Create DataFrame
    val accessLogsDF: DataFrame = accessLogsRdd.toDF()

    // 打印Schema
    /**
     * root
       |-- ipAddress: string (nullable = true)
       |-- clientIdented: string (nullable = true)
       |-- userId: string (nullable = true)
       |-- dateTime: string (nullable = true)
       |-- method: string (nullable = true)
       |-- endpoint: string (nullable = true)
       |-- protocol: string (nullable = true)
       |-- responseCode: integer (nullable = false)
       |-- contentSize: long (nullable = false)
     */
    accessLogsDF.printSchema()

    /**
     * 再次使用SQL语句进行分析，SQL语句时针对表的，所以讲DataFrame注册为一张表，然后进行SQL分析
     */
    // 将DataFrame注册为一张表
    accessLogsDF.registerTempTable("access_log")
    // 将表中的数据放入内存中
    sqlContext.cacheTable("access_log")

    /**
     * 需求一：
	      The average, min, and max content size of responses returned from the server.
     */
    val contentSizesRow = sqlContext.sql(
      """
        |SELECT
        |  SUM(contentSize), COUNT(*), MIN(contentSize), MAX(contentSize)
        |FROM
        |  access_log
      """.stripMargin).first()

    // println
    println("Content Size Avg: %s, Min: %s, Max: %s".format(
      contentSizesRow.getLong(0) / contentSizesRow.getLong(1),
      contentSizesRow(2),
      contentSizesRow(3)
    ))
    // Content Size Avg: 7777, Min: 0, Max: 138789

    /**
     * 需求二：
	        A count of response code's returned.
     */
    val responseCodeToCount: Array[(Int, Long)] = sqlContext.sql(
      """
        |SELECT
        | responseCode, COUNT(*)
        |FROM
        | access_log
        |GROUP BY
        |  responseCode
        |LIMIT
        |  10
      """.stripMargin).map(row => (row.getInt(0), row.getLong(1))).collect()
    println(
      s"""Response Code Count: ${responseCodeToCount.mkString("[",", ","]")}"""
    )

    /**
     * 需求三：
	        All IPAddresses that have accessed this server more than N times.
     */
    val ipAddresses: Array[String] = sqlContext.sql(
      """
        |SELECT
        | ipAddress, COUNT(*) AS total
        |FROM
        | access_log
        |GROUP BY
        | ipAddress
        |HAVING
        | total > 10
        |LIMIT
        | 10
      """.stripMargin).map(row => row.getString(0)).collect()
    println(
      s"""IP Addresses: ${ipAddresses.mkString("[",", ","]")}"""
    )

    /**
     * 需求四：
	        The top endpoints requested by count.
     */
    val topEndpoints: Array[(String, Long)] = sqlContext.sql(
      """
        |SELECT
        | endpoint, COUNT(*) AS total
        |FROM
        | access_log
        |GROUP BY
        | endpoint
        |ORDER BY
        | total DESC
        |LIMIT
        | 10
      """.stripMargin).map(row => (row.getString(0), row.getLong(1))).collect()
    println(
      s"""Top Endpoints: ${topEndpoints.mkString("[",", ","]")}"""
    )

    // unCache Table
    sqlContext.uncacheTable("access_log")


    /**
     * 为了调试监控
     */
    Thread.sleep(10000000)

    // SparkContext
    sc.stop()
  }


}
