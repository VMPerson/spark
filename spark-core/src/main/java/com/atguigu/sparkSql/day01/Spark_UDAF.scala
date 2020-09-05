package com.atguigu.sparkSql.day01


import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator


/**
 * @ClassName: Spark_UDAF
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/11  21:38
 * @Version: 1.0
 */
object Spark_UDAF {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_UDAF")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val df: DataFrame = spark.read.json("datas/user.json")
    df.createOrReplaceTempView("user")
    spark.udf.register("avgAge", functions.udaf(new MyAvgUDAF))

    val res: DataFrame = spark.sql("select avgAge(age) from user ")
    res.show()
    spark.stop()

  }


}



case class MyBuff(var sum: Long, var cnt: Long)

class MyAvgUDAF extends Aggregator[Long, MyBuff, Double] {

  override def zero: MyBuff = MyBuff(0, 0)

  override def reduce(b: MyBuff, a: Long): MyBuff = {
    b.sum += a
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




