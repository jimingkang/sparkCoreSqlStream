package com.ibeifeng.bigdata.spark.core

import scala.util.matching.Regex
import scala.util.matching.Regex.Match

/**
 * 数据：
 * 1.1.1.1 - - [21/Jul/2014:10:00:00 -0800] "GET /chapter1/java/src/main/java/com/databricks/apps/logs/LogAnalyzer.java HTTP/1.1" 200 1234
 */
case class ApacheAccessLog(
  ipAddress: String,
  clientIdented: String,
  userId: String,
  dateTime: String,
  method: String,
  endpoint: String,
  protocol: String,
  responseCode: Int,
  contentSize: Long )

object ApacheAccessLog{

  // regex
  val PARTTERN: Regex = """^(\S+) (-|\S+) (-|\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+)""".r


  /**
   * 对数据进行过滤
   * @param log
   * @return
   */
  def isValidateLogLine(log: String): Boolean = {
    // parse log info
    val res: Option[Match] = PARTTERN.findFirstMatchIn(log)

    // invalidate
    !res.isEmpty   // true
  }


  /**
   * 用户解析Log文件,将每行数据解析成对应的CASE CLASS
   *
   * @param log
   * @return
   */
  def parseLogLine(log: String): ApacheAccessLog = {
    // 使用正则表达式进行匹配
    // parse log info
    val res: Option[Match] = PARTTERN.findFirstMatchIn(log)

    // invalidate
    if(res.isEmpty){
      throw new RuntimeException("Cannot parse log line: " + log)
    }

    // get value
    val m: Match = res.get

    // return
    ApacheAccessLog(
      m.group(1),
      m.group(2),
      m.group(3),
      m.group(4),
      m.group(5),
      m.group(6),
      m.group(7),
      m.group(8).toInt,
      m.group(9).toLong
    )
  }


}