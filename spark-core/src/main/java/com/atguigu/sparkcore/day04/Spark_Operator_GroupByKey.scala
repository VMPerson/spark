package com.atguigu.sparkcore.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Operator_GroupByKey {

  def main(args: Array[String]): Unit = {


    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_Operator_GroupByKey"))

    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Java", "Hello Spark", "Spark Hadoop"))

    val tmp: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRdd: RDD[(String, Int)] = tmp.map((_, 1))

    //groupByKey根据key进行分组，那么只要key相同便会在一个分组中
    //方法返回的是一个元祖
    //元祖的第一个参数： key
    //元祖的第二个参数：相同key的value集合


    val gres: RDD[(String, Iterable[Int])] = mapRdd.groupByKey(2)
   /* val res: RDD[(String, Int)] = gres.map(data => {
      (data._1, data._2.sum)
    })*/
    gres.collect().foreach(println)
    sc.stop()
  }

}
