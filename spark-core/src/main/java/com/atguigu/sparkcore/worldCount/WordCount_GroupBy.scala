package com.atguigu.sparkcore.worldCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object WordCount_GroupBy {


  def main(args: Array[String]): Unit = {

    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("WordCount_GroupBy"))

    val fileRDD: RDD[String] = sc.textFile("datas/1.txt")

    val rdd: RDD[String] = fileRDD.flatMap(_.split(" "))
    val groupRDD: RDD[(String, Iterable[String])] = rdd.groupBy(str => str)
    val res: RDD[(String, Int)] = groupRDD.map(elem => {
      (elem._1, elem._2.size)
    })
    res.collect().foreach(println)

  }


}
