package com.atguigu.sparkcore.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Operator_Zip {

  def main(args: Array[String]): Unit = {


    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_Operator_Zip"))

    val rdd1: RDD[Int] = sc.makeRDD(List(1, 7, 8, 3, 4), 2)
    val rdd2: RDD[String] = sc.makeRDD(List("1", "2", "4", "3","8"), 2)

    //zip拉链操作 ，即将连个数据集的数据组装1:1的数据
    /*  有以下需要注意的地方
      1.当我们两个集合的数据量不一致时会抛以下异常
      Can only zip RDDs with same number of elements in each partition
      2.当两个数据集的分区个数不同时，也会抛出异常
      Can't zip RDDs with unequal numbers of partitions: List(2, 3)
      3.两个数据集的数据类型并没有强制性要求，数据类型可以不一样
      */


    val res: RDD[(Int, String)] = rdd1.zip(rdd2)

    res.collect().foreach(println)

  }


}
