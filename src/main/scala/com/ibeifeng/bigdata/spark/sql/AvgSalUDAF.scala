package com.ibeifeng.bigdata.spark.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
 * Created by XuanYu on 2017/3/11.
 */
object AvgSalUDAF extends UserDefinedAggregateFunction{
  /**
   * 指定数据字段的名称和类型
   *    函数的参数的名称和类型
   * @return
   */
  override def inputSchema: StructType = StructType(
    Array(StructField("sal", DoubleType, true))
  )

  /**
   * 依据需求，定义缓冲数据字段的名称和类型
   * @return
   */
  override def bufferSchema: StructType = StructType(
    StructField("sal_total", DoubleType, true) ::
      StructField("sal_count", IntegerType, true) ::
      Nil   // Nil 就相当于 空List
  )

  /**
   * 对缓冲数据中的字段进行初始化，依据实际需求
   * @param buffer
   */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, 0.0)
    buffer.update(1, 0)
  }


  /**
   * 更新缓冲数据里的值
   *
   * @param buffer
   * @param input
   */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // 获取缓冲数据的值
    val salTotal = buffer.getDouble(0)
    val salCount = buffer.getInt(1)

    // 获取传递进来的数据
    val inputSal = input.getDouble(0)

    // 更新缓冲数据的值
    buffer.update(0, salTotal + inputSal)
    buffer.update(1, salCount + 1)
  }

  /**
   * 从字面上看，就是合并的意思
   *  This is called when we merge two partially aggregated data together.
   *  表示的是
   *      对应底层来说
   *      针对RDD的不同的partition中的数据，进行分别聚合以后结果的聚合
   *
   *  Merges two aggregation buffers and stores the updated buffer values back to `buffer1`.
   *      表示合并两个聚合缓冲的数据，并将合并的缓冲数据 存储到
   *          buffer1中
   *
   * @param buffer1
   * @param buffer2
   */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    // 分别获取两个缓存区中的数据
    val salTotal1 = buffer1.getDouble(0)
    val salCount1 = buffer1.getInt(1)

    val salTotal2 = buffer2.getDouble(0)
    val salCount2 = buffer2.getInt(1)

    // 合并及更新
    buffer1.update(0, salTotal1 + salTotal2)
    buffer1.update(1, salCount1 + salCount2)
  }


  /**
   * 确定唯一性
   * @return
   */
  override def deterministic: Boolean = true

  /**
   * 决定了最后函数的输出
   * @param buffer
   * @return
   */
  override def evaluate(buffer: Row): Any = {
    val salTotal = buffer.getDouble(0)
    val salCount = buffer.getInt(1)

    // return
    salTotal / salCount
  }

  /**
   * 指定函数最后的输出数据的类型
   * @return
   */
  override def dataType: DataType = DoubleType
}
