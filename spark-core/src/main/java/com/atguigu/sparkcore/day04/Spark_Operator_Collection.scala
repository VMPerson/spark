package com.atguigu.sparkcore.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Operator_Collection {

  def main(args: Array[String]): Unit = {


    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_Operator_Collection"))

    val rdd1: RDD[Int] = sc.makeRDD(List(1, 7, 8, 3, 4))
    val rdd2: RDD[Int] = sc.makeRDD(List(1, 3, 4, 5, 2, 7))
    val rdd3: RDD[String] = sc.makeRDD(List("1", "2", "4"))

    //求两个rdd数据的交集
    //  val res: RDD[Int] = rdd1.intersection(rdd2)
    // res.collect().foreach(println)


    //求两个rdd数据集合的并集
    //  val res: RDD[Int] = rdd1.union(rdd2)
    // res.collect().foreach(println)

    //两个rdd数据的差集 ，差集有左右之分，这里注意
    //需要注意的是无论是交集、并集、还是差集，两个数据集合数据类型必须一致，不一致是无法通过编译的

    val res: RDD[Int] = rdd1.subtract(rdd2)
    res.collect().foreach(println)
  }


}
