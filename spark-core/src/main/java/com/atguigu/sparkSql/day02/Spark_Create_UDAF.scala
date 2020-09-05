package com.atguigu.sparkSql.day02

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

/**
 * @ClassName: Spark_Create_UDAF
 * @Description: TODO 强类型 + DSL 语法
 * @Author: VmPerson
 * @Date: 2020/8/13  14:59
 * @Version: 1.0
 */
object Spark_Create_UDAF {

  def main(args: Array[String]): Unit = {


    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Create_UDAF")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    val df: DataFrame = spark.read.json("datas/user.json")
    val ds: Dataset[User] = df.as[User]


    val ageAVGFuncation: MyAgeAVGFuncation = new MyAgeAVGFuncation
    ds.select(ageAVGFuncation.toColumn).show()

    spark.stop()

  }


}

case class User(name: String, age: Long)

case class MyBuff(var sum: Long, var cnt: Long)

class MyAgeAVGFuncation extends Aggregator[User, MyBuff, Double] {
  override def zero: MyBuff = MyBuff(0, 0)

  override def reduce(b: MyBuff, a: User): MyBuff = {
    b.sum += a.age
    b.cnt += 1
    b
  }

  override def merge(b1: MyBuff, b2: MyBuff): MyBuff = {
    b1.sum += b2.sum
    b1.cnt += b2.cnt
    b1
  }

  override def finish(reduction: MyBuff): Double = {
    reduction.sum.toDouble / reduction.cnt
  }

  override def bufferEncoder: Encoder[MyBuff] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}