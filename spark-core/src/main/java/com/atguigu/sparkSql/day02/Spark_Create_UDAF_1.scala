package com.atguigu.sparkSql.day02

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @ClassName: Spark_Create_UDAF_1
 * @Description: TODO   UDAF弱类型  spark3.0之后就不在推荐使用此种方式
 * @Author: VmPerson
 * @Date: 2020/8/13  20:23
 * @Version: 1.0
 */
object Spark_Create_UDAF_1 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Create_UDAF_1")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val df: DataFrame = spark.read.json("datas/user.json")
    df.createOrReplaceTempView("user")
    spark.udf.register("avgAge", new MyAvgAgeUDAF)
    spark.sql("select avgAge(age) from user").show()
    spark.stop()


  }


}


class MyAvgAgeUDAF extends UserDefinedAggregateFunction {
  //输入的数据结构
  override def inputSchema: StructType = {
    StructType(
      Array(
        StructField("age", LongType)
      )
    )
  }

  //缓冲区的数据结构
  override def bufferSchema: StructType = {
    StructType(
      Array(
        StructField("sum", LongType),
        StructField("count", LongType)
      )
    )
  }

  //返回值的数据类型
  override def dataType: DataType = {
    DoubleType
  }

  //函数稳定性
  override def deterministic: Boolean = true

  //初始化缓存区数据
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, 0L)
    buffer.update(1, 0L)
  }

  //区内数据处理逻辑
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    var newSum = buffer.getLong(0) + input.getLong(0)
    var newCount = buffer.getLong(1) + 1L
    buffer.update(0, newSum)
    buffer.update(1, newCount)

  }

  //合并缓存区数据
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var sum2 = buffer2.getLong(0)
    var count2 = buffer2.getLong(1)

    var sum1 = buffer1.getLong(0)
    var count1 = buffer1.getLong(1)

    buffer1.update(0, sum1 + sum2)
    buffer1.update(1, count1 + count2)


  }

  //返回值
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}