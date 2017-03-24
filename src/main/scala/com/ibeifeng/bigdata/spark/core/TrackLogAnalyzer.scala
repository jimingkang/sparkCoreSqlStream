package com.ibeifeng.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 基于SparkCore实现[网站流量PV和UV统计]
  */
object TrackAnalyzer{

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
      *    SparkContext用于读取数据 xuanyu original path is /user/hive/warehouse/db_track.db/yhd_log/date=20150828/
      */
  //  val trackRdd: RDD[String] = sc.textFile("/user/hive/warehouse/page_views_txt/ds=20150828/hour=18")

 val trackRdd: RDD[String] = sc.textFile("/user/hive/warehouse/page_views_txt/ds=20150828/hour=18")

    println("Count = " + trackRdd.count())       // 126134
    println(trackRdd.first())
    /**
      * Step 2: process data
      *    RDD#transformation
      */
    /**
      * 需求:
      *    统计每日的PV和UV
      *      PV：页面访问数\浏览量
      *        pv = COUNT(url)    url 不能为空, url.length > 0     第2列
      *      UV: 访客数
      *        uv = COUNT(DISTINCT guid)      第6列
      *
      *    时间：
      *       用户访问网站的时间字段
      *       trackTime        ->   2015-08-28 18:10:00      第18列
      */
    // 清洗数据
    val filteredRdd: RDD[(String, String, String)] = trackRdd
      .filter(line => line.trim.length > 0)             //  防止空字符串
      .filter(_.split("\t").size > 20)    // 过滤字段不完整的数据
      .map(line => {
      // 分割数据
      val splited = line.split("\t")
      // return  -> (date, url, guid)
      (splited(17).substring(0, 10), splited(1), splited(5) )
    })

    // 将数据进行缓存
    filteredRdd.cache()

    // 统计每日PV
    // (2015-08-28,69197)
    val pvRdd: RDD[(String, Int)] = filteredRdd
      .map{
        case(date, url, guid) => (date, url)
      }
      .filter(tuple => tuple._2.trim.length > 0)   // 判断URL不能为空
      .map{
      case (date, url) => (date, 1)
    }
      .reduceByKey(_ + _)      // 按日期进行分组统计


    // 每日统计的UV
    // (2015-08-28,39007)
    val uvRdd: RDD[(String, Int)] = filteredRdd
      .map{
        case(date, url, guid) => (date, guid)  // 提取要处理的字段
      }
      .distinct()   // 去重，相同guid在某一天的访问多次，仅仅算一次
      .map{
      case (date, guid) => (date, 1)
    }
      .reduceByKey(_ + _)      // 按日期进行分组统计

    // 将数据从内存释放
    filteredRdd.unpersist()

    /**
      * union:
      *    将RDD进行合并
      *    pvRdd 与uvRdd进行合并
      *    (2015-08-28,69197), (2015-08-28,39007)
      */
    val unionRdd = pvRdd.union(uvRdd)

    /**
      * join
      *    连接，关联
      * 涉及到JOIN
      *    必然有一个字段要进行关联，JOIN的RDD的类型必须是二元组，第一个元素为JOIN的关联ID
      *  (2015-08-28,(69197,39007))
      */
    val joinRdd: RDD[(String, (Int, Int))] = pvRdd.join(uvRdd)
    // joinRdd: org.apache.spark.rdd.RDD[(String, (Int, Int))]

    /**
      * Step 3: write data
      *    将处理的结果数据存储
      *    RDD#action
      */

    /**
      * 作业：
      *    二跳率：
	        uv(pv > 1)/uv
      */

    unionRdd.foreach(println)
    joinRdd.foreach(println)

    /**
      * 如何将结果进行保存
      */
    // 方式一：保存结果到文件中，类似于MapReduce程序将结果存于文件或者HiveQL分析将结果存于临时结果表中
    joinRdd.saveAsTextFile("/datas/spark/"+System.currentTimeMillis()+"Output")

    // 方式二：保存结果到MySQL数据库中或者到Redis中
    joinRdd.foreach(tuple => {    // foreach 是针对每条数据进行操作的
      // 比如此处向数据库中插入数据
      // 1,Connection conn =
      // 2,Insert into Table： PrepareStatement
      // 3,Close Connection
    })

    /**
      * 采用下面方式进行数据结果的导出
      */
    joinRdd.foreachPartition(iter => {
      // 比如此处向数据库中插入数据
      // 1,Connection conn
      iter.foreach(item => {
        // 2,Insert into Table： PrepareStatement
      })
      // 3,Close Connection
    })

    joinRdd.coalesce(1)  // 调整RDD分区的个数

    // =====================================================================
    sc.stop()  // 关闭资源
  }

}
