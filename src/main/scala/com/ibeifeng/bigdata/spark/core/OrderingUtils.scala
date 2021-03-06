package com.ibeifeng.bigdata.spark.core

/**
 * 自定义排序规则工具类
 */
object OrderingUtils {

  /**
   * 针对RDD[(Key, Value)]中的Value进行排序的
   */
  object SecondValueOrdering extends scala.math.Ordering[(String, Int)]{
    /**
     * 比较第二个value
     * @param x
     * @param y
     * @return
     */
    override def compare(x: (String, Int), y: (String, Int)): Int = {
      x._2.compare(y._2)
    }
  }

}
