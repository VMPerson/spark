package com.atguigu.sparkcore.worldCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount_AggregateByKey {

  def main(args: Array[String]): Unit = {


    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("WordCount_AggregateByKey"))
    val fileRDD: RDD[String] = sc.textFile("datas/1.txt")
    val rdd: RDD[(String, Int)] = fileRDD.flatMap(_.split(" ")).map((_, 1))
    val res: RDD[(String, Int)] = rdd.aggregateByKey(0)(_ + _, _ + _)
    res.collect().foreach(println)


  }


}
