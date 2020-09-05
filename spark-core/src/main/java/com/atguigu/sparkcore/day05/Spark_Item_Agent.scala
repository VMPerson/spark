package com.atguigu.sparkcore.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark_Item_Agent
 * @Description: TODO  日志统计练习
 * @Author: VmPerson
 * @Date: 2020/8/6  17:20
 * @Version: 1.0
 */
object Spark_Item_Agent {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_Item_Agent"))
    val fileRDD: RDD[String] = sc.textFile("datas/agent.log")

    val rdd: RDD[((String, String), Int)] = fileRDD.map(data => {
      var tmp = data.split(" ")
      ((tmp(1), tmp(4)), 1)
    })
    val redRDD: RDD[((String, String), Int)] = rdd.reduceByKey(_ + _)

    val mapRdd: RDD[(String, (String, Int))] = redRDD.map {
      case ((pro, adId), count) => {
        (pro, (adId, count))
      }
    }
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRdd.groupByKey()

    val res: RDD[(String, List[(String, Int)])] = groupRDD.map {
      case (pro, list) => {
        val tmp: List[(String, Int)] = list.toList.sortWith((x, y) => x._2 > y._2).take(3)
        (pro, tmp)
      }
    }
    res.collect().foreach(println)
  }


}
