package com.atguigu.sparkcore.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Operator_SortBy {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_Operator_SortBy"))

    //sortBy 排序  有两个参数
    //第一个参数：  数据处理函数，如果是int类型直接num=>num，如果是其他类型需要用一个函数做处理
    //第二个参数：  不写或者true则是正序，false 则是倒序

    /* 整数集合排序
       val rdd: RDD[Int] = sc.makeRDD(List(1, 4, 3, 2, 6, 2), 2)
       val res: RDD[Int] = rdd.sortBy(num => num, true)*/

    //对一个字符串集合做排序
    val rdd: RDD[String] = sc.makeRDD(List("1", "2", "7", "10", "12"), 2)
    val res: RDD[String] = rdd.sortBy(num => {
      num.toInt
    }, false)

    res.collect().foreach(println)

  }


}
