package com.atguigu.sparkcore.worldCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount_ReduceByKey {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("WordCount_ReduceByKey"))

    val fileRDD: RDD[String] = sc.textFile("datas/1.txt")

    val rdd: RDD[(String, Int)] = fileRDD.flatMap(_.split(" ")).map((_, 1))
    val res: RDD[(String, Int)] = rdd.reduceByKey(_ + _)
    res.collect().foreach(println)
    sc.stop()


  }


}
