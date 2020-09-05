package com.atguigu.sparkcore.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Operator_Glom {

  //将所有数据珍禾味一个整体进行处理
  def main(args: Array[String]): Unit = {


    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_Operator_Glom"))

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    val res: RDD[Array[Int]] = rdd.glom()
    res.collect.foreach(arr => {
     println( arr.max)
    })
    sc.stop()

  }


}
