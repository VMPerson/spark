package com.atguigu.sparkcore.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Operator_ReduceByKey {

  def main(args: Array[String]): Unit = {


    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_Operator_ReduceByKey"))

    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Java", "Hello Spark"))

    val tmp: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRdd: RDD[(String, Int)] = tmp.map((_, 1))
    //reduceByKey 会将相同的key的数据分组在一起，然后将分组后的value进行reduce聚合操作，最终获取结果
    val res: RDD[(String, Int)] = mapRdd.reduceByKey(_ + _)
    res.collect().foreach(println)
    sc.stop()


  }


}
